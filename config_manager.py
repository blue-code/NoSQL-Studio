import json
import os
from datetime import datetime
from typing import Dict, List, Optional


class ConfigManager:
    def __init__(self, config_file='db_config.json'):
        self.config_file = config_file
        self.config = self.load_config()

    def load_config(self) -> Dict:
        """Load configuration from file"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading config: {e}")
                return self.get_default_config()
        return self.get_default_config()

    def get_default_config(self) -> Dict:
        """Get default configuration"""
        return {
            'mongo_profiles': [],
            'redis_profiles': [],
            'query_history': {
                'mongo': [],
                'redis': []
            },
            'favorites': {
                'mongo': [],
                'redis': []
            },
            'settings': {
                'theme': 'light',
                'auto_refresh': False,
                'refresh_interval': 30,
                'max_history': 50,
                'page_size': 100
            },
            'last_connection': {
                'mongo': None,
                'redis': None
            }
        }

    def save_config(self):
        """Save configuration to file"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving config: {e}")

    # MongoDB Profiles
    def add_mongo_profile(self, name: str, host: str, port: int,
                         username: str = '', password: str = '', database: str = ''):
        """Add MongoDB connection profile"""
        profile = {
            'name': name,
            'host': host,
            'port': port,
            'username': username,
            'password': password,
            'database': database,
            'created_at': datetime.now().isoformat()
        }

        # Check if profile with same name exists
        existing = [p for p in self.config['mongo_profiles'] if p['name'] == name]
        if existing:
            # Update existing
            idx = self.config['mongo_profiles'].index(existing[0])
            self.config['mongo_profiles'][idx] = profile
        else:
            self.config['mongo_profiles'].append(profile)

        self.save_config()

    def get_mongo_profiles(self) -> List[Dict]:
        """Get all MongoDB profiles"""
        return self.config.get('mongo_profiles', [])

    def delete_mongo_profile(self, name: str):
        """Delete MongoDB profile"""
        self.config['mongo_profiles'] = [
            p for p in self.config['mongo_profiles'] if p['name'] != name
        ]
        self.save_config()

    # Redis Profiles
    def add_redis_profile(self, name: str, host: str, port: int,
                         password: str = '', db: int = 0):
        """Add Redis connection profile"""
        profile = {
            'name': name,
            'host': host,
            'port': port,
            'password': password,
            'db': db,
            'created_at': datetime.now().isoformat()
        }

        # Check if profile with same name exists
        existing = [p for p in self.config['redis_profiles'] if p['name'] == name]
        if existing:
            # Update existing
            idx = self.config['redis_profiles'].index(existing[0])
            self.config['redis_profiles'][idx] = profile
        else:
            self.config['redis_profiles'].append(profile)

        self.save_config()

    def get_redis_profiles(self) -> List[Dict]:
        """Get all Redis profiles"""
        return self.config.get('redis_profiles', [])

    def delete_redis_profile(self, name: str):
        """Delete Redis profile"""
        self.config['redis_profiles'] = [
            p for p in self.config['redis_profiles'] if p['name'] != name
        ]
        self.save_config()

    # Query History
    def add_to_history(self, db_type: str, query: str, database: str = '',
                      collection: str = '', execution_time: float = 0):
        """Add query to history"""
        history_item = {
            'query': query,
            'database': database,
            'collection': collection,
            'execution_time': execution_time,
            'timestamp': datetime.now().isoformat()
        }

        if db_type not in self.config['query_history']:
            self.config['query_history'][db_type] = []

        self.config['query_history'][db_type].insert(0, history_item)

        # Limit history size
        max_history = self.config['settings'].get('max_history', 50)
        self.config['query_history'][db_type] = \
            self.config['query_history'][db_type][:max_history]

        self.save_config()

    def get_history(self, db_type: str) -> List[Dict]:
        """Get query history"""
        return self.config['query_history'].get(db_type, [])

    def clear_history(self, db_type: str):
        """Clear query history"""
        self.config['query_history'][db_type] = []
        self.save_config()

    # Favorites
    def add_favorite(self, db_type: str, name: str, query: str,
                    database: str = '', collection: str = ''):
        """Add query to favorites"""
        favorite = {
            'name': name,
            'query': query,
            'database': database,
            'collection': collection,
            'created_at': datetime.now().isoformat()
        }

        if db_type not in self.config['favorites']:
            self.config['favorites'][db_type] = []

        # Check if favorite with same name exists
        existing = [f for f in self.config['favorites'][db_type] if f['name'] == name]
        if existing:
            idx = self.config['favorites'][db_type].index(existing[0])
            self.config['favorites'][db_type][idx] = favorite
        else:
            self.config['favorites'][db_type].append(favorite)

        self.save_config()

    def get_favorites(self, db_type: str) -> List[Dict]:
        """Get favorites"""
        return self.config['favorites'].get(db_type, [])

    def delete_favorite(self, db_type: str, name: str):
        """Delete favorite"""
        if db_type in self.config['favorites']:
            self.config['favorites'][db_type] = [
                f for f in self.config['favorites'][db_type] if f['name'] != name
            ]
            self.save_config()

    # Settings
    def update_setting(self, key: str, value):
        """Update setting"""
        self.config['settings'][key] = value
        self.save_config()

    def get_setting(self, key: str, default=None):
        """Get setting"""
        return self.config['settings'].get(key, default)

    # Last Connection
    def set_last_connection(self, db_type: str, profile_name: str):
        """Set last used connection"""
        self.config['last_connection'][db_type] = profile_name
        self.save_config()

    def get_last_connection(self, db_type: str) -> Optional[str]:
        """Get last used connection"""
        return self.config['last_connection'].get(db_type)
