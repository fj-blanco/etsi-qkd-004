import uuid
import json
import os
import time
import fcntl
import logging
from threading import Thread, Lock

class KSIDManager:
    """
    Key Stream ID Manager for QKD systems implementing ETSI GS QKD 004 functionality.
    This is a lightweight implementation that maintains state in a shared JSON file.
    """
    def __init__(self, state_file_path="/dev/shm/ksid_state.json"):
        self.state_file_path = state_file_path
        self.lock = Lock()
        
        # In-memory cache to avoid excessive file operations
        self.ksid_cache = {}
        self.last_update_time = 0
        
        self.ensure_state_file()
        
        # Start background cleanup task
        self.cleanup_thread = Thread(target=self._cleanup_expired_ksids, daemon=True)
        self.cleanup_thread.start()
        
        logging.info(f"KSID Manager initialized with state file: {state_file_path}")
        
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
                self._refresh_cache()
        except Exception as e:
            logging.error(f"Error ensuring state file: {e}")
            # Create an in-memory only state if file cannot be created
            self.ksid_cache = {}
            self.last_update_time = time.time()
    
    def _refresh_cache(self):
        """Refresh the in-memory cache from the state file."""
        try:
            state = self.get_state()
            self.ksid_cache = state.get("ksid_map", {})
            self.last_update_time = state.get("last_updated", time.time())
        except Exception as e:
            logging.error(f"Error refreshing cache: {e}")
    
    def get_state(self):
        """Get the current state from the state file with shared lock."""
        try:
            with open(self.state_file_path, 'r') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                try:
                    state = json.load(f)
                except json.JSONDecodeError:
                    logging.error("Error decoding state file, creating new state")
                    state = {
                        "ksid_map": {},
                        "last_updated": time.time()
                    }
                finally:
                    fcntl.flock(f, fcntl.LOCK_UN)
            return state
        except Exception as e:
            logging.error(f"Error reading state file: {e}")
            return {"ksid_map": {}, "last_updated": time.time()}
    
    def update_state(self, state):
        """Update the state file with exclusive lock."""
        try:
            with open(self.state_file_path, 'w') as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                json.dump(state, f)
                fcntl.flock(f, fcntl.LOCK_UN)
            
            # Update in-memory cache
            self.ksid_cache = state.get("ksid_map", {})
            self.last_update_time = state.get("last_updated", time.time())
        except Exception as e:
            logging.error(f"Error updating state file: {e}")
    
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
            # Ensure cache is up to date
            self._refresh_cache()
            
            # Case 1: Generate new KSID (null KSID in request)
            if requested_ksid is None or (isinstance(requested_ksid, uuid.UUID) and requested_ksid == uuid.UUID(int=0)):
                new_ksid = uuid.uuid4()
                # Ensure uniqueness
                while str(new_ksid) in self.ksid_cache:
                    new_ksid = uuid.uuid4()
                    
                # Create entry in cache
                self.ksid_cache[str(new_ksid)] = {
                    "creation_time": time.time(),
                    "ttl": ttl,
                    "last_index": 0,
                    "source_uri": source_uri,
                    "dest_uri": dest_uri,
                    "last_accessed": time.time()
                }
                
                # Update state file
                state = self.get_state()
                state["ksid_map"] = self.ksid_cache
                state["last_updated"] = time.time()
                self.update_state(state)
                
                logging.info(f"Allocated new KSID: {new_ksid}")
                return new_ksid, 0, 0  # Return UUID object, not string
                
            # Case 3: Use predefined KSID
            else:
                # Convert UUID to string for dictionary lookup
                ksid_str = str(requested_ksid)
                
                if ksid_str in self.ksid_cache:
                    # Verify URIs match if provided
                    entry = self.ksid_cache[ksid_str]
                    if (source_uri and dest_uri and 
                        (entry["source_uri"] != source_uri or entry["dest_uri"] != dest_uri)):
                        logging.error(f"KSID {requested_ksid} exists but URIs don't match")
                        return None, 0, 3  # KSID in use with different URIs
                    
                    # Update last access time
                    entry["last_accessed"] = time.time()
                    self.ksid_cache[ksid_str] = entry
                    
                    # Update state file
                    state = self.get_state()
                    state["ksid_map"] = self.ksid_cache
                    state["last_updated"] = time.time()
                    self.update_state(state)
                    
                    logging.info(f"Using existing KSID: {requested_ksid}, index: {entry['last_index']}")
                    return requested_ksid, entry["last_index"], 0  # Return the original UUID object
                else:
                    # New KSID registration with client-provided ID
                    self.ksid_cache[ksid_str] = {
                        "creation_time": time.time(),
                        "last_accessed": time.time(),
                        "ttl": ttl,
                        "last_index": 0,
                        "source_uri": source_uri,
                        "dest_uri": dest_uri
                    }
                    
                    # Update state file
                    state = self.get_state()
                    state["ksid_map"] = self.ksid_cache
                    state["last_updated"] = time.time()
                    self.update_state(state)
                    
                    logging.info(f"Allocated requested KSID: {requested_ksid}")
                    return requested_ksid, 0, 0  # Return the original UUID object
    
    def update_index(self, ksid, new_index):
        """Update the index for a KSID."""
        with self.lock:
            # Ensure cache is up to date
            self._refresh_cache()
            
            ksid_str = str(ksid)
            if ksid_str in self.ksid_cache:
                self.ksid_cache[ksid_str]["last_index"] = new_index
                self.ksid_cache[ksid_str]["last_accessed"] = time.time()
                
                # Update state file
                state = self.get_state()
                state["ksid_map"] = self.ksid_cache
                state["last_updated"] = time.time()
                self.update_state(state)
                
                logging.debug(f"Updated index for KSID {ksid} to {new_index}")
                return True
            logging.error(f"KSID {ksid} not found for index update")
            return False
    
    def get_ksid_info(self, ksid):
        """Get information about a KSID."""
        with self.lock:
            # Ensure cache is up to date
            self._refresh_cache()
            
            ksid_str = str(ksid)
            if ksid_str in self.ksid_cache:
                info = dict(self.ksid_cache[ksid_str])  # Create a copy to avoid modifying cache directly
                
                # Update last accessed time
                self.ksid_cache[ksid_str]["last_accessed"] = time.time()
                
                # Update state file
                state = self.get_state()
                state["ksid_map"] = self.ksid_cache
                state["last_updated"] = time.time()
                self.update_state(state)
                
                return info
            return None
            
    def close_ksid(self, ksid):
        """Close a KSID and release its resources."""
        with self.lock:
            # Ensure cache is up to date
            self._refresh_cache()
            
            ksid_str = str(ksid)
            if ksid_str in self.ksid_cache:
                del self.ksid_cache[ksid_str]
                
                # Update state file
                state = self.get_state()
                state["ksid_map"] = self.ksid_cache
                state["last_updated"] = time.time()
                self.update_state(state)
                
                logging.info(f"Closed KSID: {ksid}")
                return 0  # Success
            logging.error(f"KSID {ksid} not found for closing")
            return 1  # KSID not found
    
    def _cleanup_expired_ksids(self):
        """Background task to clean up expired KSIDs."""
        while True:
            try:
                with self.lock:
                    # Ensure cache is up to date
                    self._refresh_cache()
                    
                    current_time = time.time()
                    
                    # Find expired KSIDs
                    expired_ksids = []
                    for ksid, entry in self.ksid_cache.items():
                        expiration_time = entry["creation_time"] + entry["ttl"]
                        if current_time > expiration_time:
                            expired_ksids.append(ksid)
                    
                    # Remove expired KSIDs
                    if expired_ksids:
                        for ksid in expired_ksids:
                            if ksid in self.ksid_cache:
                                del self.ksid_cache[ksid]
                        
                        # Update state file
                        state = self.get_state()
                        state["ksid_map"] = self.ksid_cache
                        state["last_updated"] = current_time
                        self.update_state(state)
                        
                        logging.info(f"Cleaned up {len(expired_ksids)} expired KSIDs")
            
            except Exception as e:
                logging.error(f"Error in KSID cleanup task: {e}")
            
            # Sleep for a while before next cleanup
            time.sleep(60)  # Check every minute

# Simple initialization for testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(message)s')
    ksid_manager = KSIDManager()
    logging.info("KSIDManager initialized and ready for use")