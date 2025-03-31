import uuid
import json
import os
import time
import fcntl
import logging
from threading import Thread, RLock

class KSIDManager:
    """
    Key Stream ID Manager for QKD systems implementing ETSI GS QKD 004 functionality.
    This is a lightweight implementation that maintains state in a shared JSON file.
    """
    def __init__(self, state_file_path="/dev/shm/ksid_state.json", shared_state=None):
        self.state_file_path = state_file_path
        self.lock = RLock()
        
        # If shared_state is provided, use it for multiprocessing support
        self.is_multiprocessing = shared_state is not None
        
        # In-memory cache to avoid excessive file operations
        if self.is_multiprocessing:
            self.ksid_cache = shared_state  # Use shared dictionary for multiprocessing
        else:
            self.ksid_cache = {}  # Use local dictionary for single process
            
        self.last_update_time = 0
        self.dirty = False
        self.last_persisted_time = 0
        self.persist_interval = 5
        
        self.ensure_state_file()
        
        # Start background tasks only in single-process mode
        self.running = True
        if not self.is_multiprocessing:
            self.persist_thread = Thread(target=self._periodic_persist, daemon=True)
            self.persist_thread.start()
            
            self.cleanup_thread = Thread(target=self._cleanup_expired_ksids, daemon=True)
            self.cleanup_thread.start()
        
        logging.info(f"KSID Manager initialized with state file: {state_file_path}")
    
    def __del__(self):
        """Ensure background threads are stopped when the object is deleted."""
        self.running = False
        self._persist_state_if_needed(force=True)
        
    def ensure_state_file(self):
        """Create the state file if it doesn't exist."""
        try:
            if not os.path.exists(self.state_file_path):
                with open(self.state_file_path, 'w') as f:
                    fcntl.flock(f, fcntl.LOCK_EX)
                    json.dump({
                        "ksid_map": {},
                        "last_updated": time.time()
                    }, f)
                    fcntl.flock(f, fcntl.LOCK_UN)
                logging.info(f"Created new KSID state file at {self.state_file_path}")
            else:
                # Load initial state into cache
                self._load_state_from_file()
        except Exception as e:
            logging.error(f"Error ensuring state file: {e}")
            # Create an in-memory only state if file cannot be created
            self.ksid_cache = {}
            self.last_update_time = time.time()
    
    def _load_state_from_file(self):
        """Load state from file to cache."""
        try:
            with open(self.state_file_path, 'r') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                try:
                    state = json.load(f)
                    
                    with self.lock:
                        self.ksid_cache = state.get("ksid_map", {})
                        self.last_update_time = state.get("last_updated", time.time())
                        self.last_persisted_time = time.time()
                        self.dirty = False
                    
                    logging.debug(f"Loaded {len(self.ksid_cache)} KSIDs from state file")
                except json.JSONDecodeError:
                    logging.error("Error decoding state file, creating new state")
                    with self.lock:
                        self.ksid_cache = {}
                        self.last_update_time = time.time()
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
        except Exception as e:
            logging.error(f"Error loading state from file: {e}")
            with self.lock:
                self.ksid_cache = {}
                self.last_update_time = time.time()
    
    def _persist_state_if_needed(self, force=False):
        """Persist state to file if needed."""
        current_time = time.time()
        
        should_persist = False
        state_to_save = None
        
        with self.lock:
            if (force or self.dirty) and (current_time - self.last_persisted_time >= self.persist_interval):
                # Prepare the state to save
                state_to_save = {
                    "ksid_map": dict(self.ksid_cache),  # Create a copy of the dict
                    "last_updated": current_time
                }
                
                self.last_persisted_time = current_time
                self.dirty = False
                should_persist = True
        
        # Only perform file I/O outside the lock
        if should_persist and state_to_save:
            try:
                with open(self.state_file_path, 'w') as f:
                    fcntl.flock(f, fcntl.LOCK_EX)
                    json.dump(state_to_save, f)
                    fcntl.flock(f, fcntl.LOCK_UN)
                
                logging.debug(f"Persisted {len(state_to_save['ksid_map'])} KSIDs to state file")
            except Exception as e:
                logging.error(f"Error persisting state: {e}")
                with self.lock:
                    self.dirty = True  # Mark dirty again so we try again later
    
    def _periodic_persist(self):
        """Background task to periodically persist state."""
        while self.running:
            try:
                self._persist_state_if_needed()
            except Exception as e:
                logging.error(f"Error in periodic persist: {e}")
            
            # Sleep with short intervals to allow for clean shutdown
            for _ in range(5):  # 5 x 0.2 = 1 second
                if not self.running:
                    break
                time.sleep(0.2)
    
    def allocate_ksid(self, requested_ksid=None, source_uri=None, dest_uri=None, ttl=7200):
        """
        Allocate a KSID for key synchronization.
        
        Args:
            requested_ksid: UUID object or None. If None, a new KSID is generated.
            source_uri: Source URI for this connection
            dest_uri: Destination URI for this connection
            ttl: Time-to-live in seconds for this KSID
            
        Returns:
            tuple: (ksid, index, status)
                - ksid: UUID object representing the allocated KSID
                - index: Current index for this KSID
                - status: 0 for success, other for errors
        """
        with self.lock:
            current_time = time.time()
            
            # Case 1: Generate new KSID (null KSID in request)
            if requested_ksid is None or (isinstance(requested_ksid, uuid.UUID) and requested_ksid == uuid.UUID(int=0)):
                new_ksid = uuid.uuid4()
                
                # Ensure uniqueness - convert to string for dictionary key
                ksid_str = str(new_ksid)
                while ksid_str in self.ksid_cache:
                    new_ksid = uuid.uuid4()
                    ksid_str = str(new_ksid)
                    
                # Create entry in cache
                entry = {
                    "creation_time": current_time,
                    "ttl": ttl,
                    "last_index": 0,
                    "source_uri": source_uri,
                    "dest_uri": dest_uri,
                    "last_accessed": current_time
                }
                
                # For shared dictionaries in multiprocessing, we need to ensure
                # atomicity of updates by setting the entire entry at once
                self.ksid_cache[ksid_str] = entry
                
                self.dirty = True
                logging.info(f"Allocated new KSID: {new_ksid}")
                
                # For multiprocessing, make sure to persist immediately 
                if hasattr(self, 'is_multiprocessing') and self.is_multiprocessing:
                    # Direct file write for multiprocessing
                    try:
                        state = {
                            "ksid_map": dict(self.ksid_cache),
                            "last_updated": current_time
                        }
                        with open(self.state_file_path, 'w') as f:
                            fcntl.flock(f, fcntl.LOCK_EX)
                            json.dump(state, f)
                            fcntl.flock(f, fcntl.LOCK_UN)
                    except Exception as e:
                        logging.error(f"Error persisting state: {e}")
                else:
                    # Use normal persistence for single process
                    self._persist_state_if_needed(force=True)
                
                return new_ksid, 0, 0  # Return UUID object, not string
                
            # Case 2: Use predefined KSID
            else:
                # Convert UUID to string for dictionary lookup
                ksid_str = str(requested_ksid)
                
                if ksid_str in self.ksid_cache:
                    # Verify URIs match
                    existing_entry = self.ksid_cache[ksid_str]
                    if (source_uri and dest_uri and 
                        (existing_entry["source_uri"] != source_uri or existing_entry["dest_uri"] != dest_uri)):
                        logging.error(f"KSID {requested_ksid} exists but URIs don't match")
                        return None, 0, 3
                    
                    # For shared dictionary, create a new entry with updated access time
                    entry = dict(existing_entry)  # Create a copy
                    entry["last_accessed"] = current_time
                    
                    # Update the entire entry atomically
                    self.ksid_cache[ksid_str] = entry
                    
                    self.dirty = True
                    last_index = entry["last_index"]
                    logging.info(f"Using existing KSID: {requested_ksid}, index: {last_index}")
                    
                    return requested_ksid, last_index, 0
                else:
                    # New KSID registration with client-provided ID
                    entry = {
                        "creation_time": current_time,
                        "last_accessed": current_time,
                        "ttl": ttl,
                        "last_index": 0,
                        "source_uri": source_uri,
                        "dest_uri": dest_uri
                    }
                    
                    # Set the entry atomically
                    self.ksid_cache[ksid_str] = entry
                    
                    self.dirty = True
                    logging.info(f"Allocated requested KSID: {requested_ksid}")
                    
                    # For multiprocessing, make sure to persist immediately
                    if hasattr(self, 'is_multiprocessing') and self.is_multiprocessing:
                        # Direct file write for multiprocessing
                        try:
                            state = {
                                "ksid_map": dict(self.ksid_cache),
                                "last_updated": current_time
                            }
                            with open(self.state_file_path, 'w') as f:
                                fcntl.flock(f, fcntl.LOCK_EX)
                                json.dump(state, f)
                                fcntl.flock(f, fcntl.LOCK_UN)
                        except Exception as e:
                            logging.error(f"Error persisting state: {e}")
                    else:
                        # Use normal persistence for single process
                        self._persist_state_if_needed(force=True)
                    
                    return requested_ksid, 0, 0
    
    def update_index(self, ksid, new_index):
        """Update the index for a KSID."""
        with self.lock:
            ksid_str = str(ksid)
            if ksid_str in self.ksid_cache:
                # Make a copy of the entry
                entry = dict(self.ksid_cache[ksid_str])
                
                # Update the entry
                entry["last_index"] = new_index
                entry["last_accessed"] = time.time()
                self.ksid_cache[ksid_str] = entry
                
                self.dirty = True
                logging.debug(f"Updated index for KSID {ksid} to {new_index}")
                return True
            
            logging.error(f"KSID {ksid} not found for index update")
            return False
    
    def get_ksid_info(self, ksid):
        """Get information about a KSID."""
        with self.lock:
            ksid_str = str(ksid)
            if ksid_str in self.ksid_cache:
                # Create a copy of the info
                info = dict(self.ksid_cache[ksid_str])
                
                # Update last accessed time
                entry = dict(self.ksid_cache[ksid_str])
                entry["last_accessed"] = time.time()
                self.ksid_cache[ksid_str] = entry
                
                self.dirty = True
                return info
            
            logging.warning(f"KSID {ksid} not found in cache")
            return None
            
    def close_ksid(self, ksid):
        """Close a KSID and release its resources."""
        with self.lock:
            ksid_str = str(ksid)
            if ksid_str in self.ksid_cache:
                del self.ksid_cache[ksid_str]
                self.dirty = True
                
                # Try to persist immediately for important operations
                self._persist_state_if_needed(force=True)
                
                logging.info(f"Closed KSID: {ksid}")
                return 0  # Success
                
            logging.error(f"KSID {ksid} not found for closing")
            return 1  # KSID not found
    
    def _cleanup_expired_ksids(self):
        """Background task to clean up expired KSIDs."""
        while self.running:
            try:
                # Sleep first to allow initialization to complete
                # Sleep with short intervals to allow for clean shutdown
                for _ in range(300):  # 300 x 0.2 = 60 seconds
                    if not self.running:
                        break
                    time.sleep(0.2)
                
                if not self.running:
                    break
                
                # Find expired KSIDs
                ksids_to_remove = []
                current_time = time.time()
                
                with self.lock:
                    for ksid, entry in self.ksid_cache.items():
                        expiration_time = entry["creation_time"] + entry["ttl"]
                        if current_time > expiration_time:
                            ksids_to_remove.append(ksid)
                
                # Only lock when actually removing items
                if ksids_to_remove:
                    with self.lock:
                        expired_count = 0
                        for ksid in ksids_to_remove:
                            if ksid in self.ksid_cache:
                                del self.ksid_cache[ksid]
                                expired_count += 1
                        
                        if expired_count > 0:
                            self.dirty = True
                            logging.info(f"Cleaned up {expired_count} expired KSIDs")
            
            except Exception as e:
                logging.error(f"Error in KSID cleanup task: {e}")