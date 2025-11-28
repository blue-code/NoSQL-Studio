import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import redis
import json
from collections import defaultdict


class DatabaseQueryTool:
    def __init__(self, root):
        self.root = root
        self.root.title("MongoDB & Redis Query Tool")
        self.root.geometry("1400x800")

        self.mongo_client = None
        self.redis_client = None

        self.setup_ui()

    def setup_ui(self):
        # Create notebook for tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=10, pady=10)

        # MongoDB Tab
        self.mongo_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.mongo_frame, text='MongoDB')
        self.setup_mongo_tab()

        # Redis Tab
        self.redis_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.redis_frame, text='Redis')
        self.setup_redis_tab()

    def setup_mongo_tab(self):
        # Connection Frame
        conn_frame = ttk.LabelFrame(self.mongo_frame, text="MongoDB Connection", padding=10)
        conn_frame.pack(fill='x', padx=10, pady=10)

        ttk.Label(conn_frame, text="Host:").grid(row=0, column=0, sticky='w', padx=5, pady=5)
        self.mongo_host = ttk.Entry(conn_frame, width=20)
        self.mongo_host.insert(0, "localhost")
        self.mongo_host.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(conn_frame, text="Port:").grid(row=0, column=2, sticky='w', padx=5, pady=5)
        self.mongo_port = ttk.Entry(conn_frame, width=10)
        self.mongo_port.insert(0, "27017")
        self.mongo_port.grid(row=0, column=3, padx=5, pady=5)

        ttk.Label(conn_frame, text="Username:").grid(row=0, column=4, sticky='w', padx=5, pady=5)
        self.mongo_username = ttk.Entry(conn_frame, width=15)
        self.mongo_username.grid(row=0, column=5, padx=5, pady=5)

        ttk.Label(conn_frame, text="Password:").grid(row=0, column=6, sticky='w', padx=5, pady=5)
        self.mongo_password = ttk.Entry(conn_frame, width=15, show="*")
        self.mongo_password.grid(row=0, column=7, padx=5, pady=5)

        ttk.Button(conn_frame, text="Connect", command=self.connect_mongo).grid(row=0, column=8, padx=5, pady=5)
        ttk.Button(conn_frame, text="Refresh", command=self.refresh_mongo_tree).grid(row=0, column=9, padx=5, pady=5)

        self.mongo_status = ttk.Label(conn_frame, text="Status: Disconnected", foreground="red")
        self.mongo_status.grid(row=1, column=0, columnspan=10, pady=5)

        # Main content frame with paned window
        content_frame = ttk.Frame(self.mongo_frame)
        content_frame.pack(fill='both', expand=True, padx=10, pady=10)

        paned = ttk.PanedWindow(content_frame, orient='horizontal')
        paned.pack(fill='both', expand=True)

        # Left panel - Database Browser
        left_frame = ttk.LabelFrame(paned, text="Database Browser", padding=10)
        paned.add(left_frame, weight=1)

        # Treeview for databases and collections
        tree_scroll = ttk.Scrollbar(left_frame)
        tree_scroll.pack(side='right', fill='y')

        self.mongo_tree = ttk.Treeview(left_frame, yscrollcommand=tree_scroll.set, selectmode='browse')
        self.mongo_tree.pack(side='left', fill='both', expand=True)
        tree_scroll.config(command=self.mongo_tree.yview)

        self.mongo_tree.heading('#0', text='Databases & Collections')
        self.mongo_tree.bind('<<TreeviewSelect>>', self.on_mongo_tree_select)

        # Right panel - Query and Results
        right_frame = ttk.Frame(paned)
        paned.add(right_frame, weight=2)

        # Query Frame
        query_frame = ttk.LabelFrame(right_frame, text="Query", padding=10)
        query_frame.pack(fill='both', expand=True)

        # Database and Collection info
        info_frame = ttk.Frame(query_frame)
        info_frame.pack(fill='x', pady=5)

        ttk.Label(info_frame, text="Database:").grid(row=0, column=0, sticky='w', padx=5, pady=2)
        self.mongo_database = ttk.Entry(info_frame, width=30)
        self.mongo_database.grid(row=0, column=1, padx=5, pady=2, sticky='w')

        ttk.Label(info_frame, text="Collection:").grid(row=0, column=2, sticky='w', padx=5, pady=2)
        self.mongo_collection = ttk.Entry(info_frame, width=30)
        self.mongo_collection.grid(row=0, column=3, padx=5, pady=2, sticky='w')

        ttk.Button(info_frame, text="Show Schema", command=self.show_mongo_schema).grid(row=0, column=4, padx=5, pady=2)

        # Query input
        ttk.Label(query_frame, text="Query (JSON):").pack(anchor='w', padx=5, pady=5)
        self.mongo_query = scrolledtext.ScrolledText(query_frame, width=60, height=6)
        self.mongo_query.insert('1.0', '{}')
        self.mongo_query.pack(fill='x', padx=5, pady=5)

        # Query options
        options_frame = ttk.Frame(query_frame)
        options_frame.pack(fill='x', pady=5)

        ttk.Label(options_frame, text="Limit:").pack(side='left', padx=5)
        self.mongo_limit = ttk.Entry(options_frame, width=10)
        self.mongo_limit.insert(0, "10")
        self.mongo_limit.pack(side='left', padx=5)

        ttk.Button(options_frame, text="Execute Query", command=self.execute_mongo_query).pack(side='left', padx=10)

        # Results
        ttk.Label(query_frame, text="Results:").pack(anchor='w', padx=5, pady=5)
        self.mongo_result = scrolledtext.ScrolledText(query_frame, width=80, height=20)
        self.mongo_result.pack(fill='both', expand=True, padx=5, pady=5)

    def setup_redis_tab(self):
        # Connection Frame
        conn_frame = ttk.LabelFrame(self.redis_frame, text="Redis Connection", padding=10)
        conn_frame.pack(fill='x', padx=10, pady=10)

        ttk.Label(conn_frame, text="Host:").grid(row=0, column=0, sticky='w', padx=5, pady=5)
        self.redis_host = ttk.Entry(conn_frame, width=20)
        self.redis_host.insert(0, "localhost")
        self.redis_host.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(conn_frame, text="Port:").grid(row=0, column=2, sticky='w', padx=5, pady=5)
        self.redis_port = ttk.Entry(conn_frame, width=10)
        self.redis_port.insert(0, "6379")
        self.redis_port.grid(row=0, column=3, padx=5, pady=5)

        ttk.Label(conn_frame, text="Password:").grid(row=0, column=4, sticky='w', padx=5, pady=5)
        self.redis_password = ttk.Entry(conn_frame, width=15, show="*")
        self.redis_password.grid(row=0, column=5, padx=5, pady=5)

        ttk.Label(conn_frame, text="DB:").grid(row=0, column=6, sticky='w', padx=5, pady=5)
        self.redis_db = ttk.Entry(conn_frame, width=10)
        self.redis_db.insert(0, "0")
        self.redis_db.grid(row=0, column=7, padx=5, pady=5)

        ttk.Button(conn_frame, text="Connect", command=self.connect_redis).grid(row=0, column=8, padx=5, pady=5)
        ttk.Button(conn_frame, text="Refresh", command=self.refresh_redis_tree).grid(row=0, column=9, padx=5, pady=5)

        self.redis_status = ttk.Label(conn_frame, text="Status: Disconnected", foreground="red")
        self.redis_status.grid(row=1, column=0, columnspan=10, pady=5)

        # Main content frame with paned window
        content_frame = ttk.Frame(self.redis_frame)
        content_frame.pack(fill='both', expand=True, padx=10, pady=10)

        paned = ttk.PanedWindow(content_frame, orient='horizontal')
        paned.pack(fill='both', expand=True)

        # Left panel - Key Browser
        left_frame = ttk.LabelFrame(paned, text="Key Browser", padding=10)
        paned.add(left_frame, weight=1)

        # Search frame
        search_frame = ttk.Frame(left_frame)
        search_frame.pack(fill='x', pady=5)

        ttk.Label(search_frame, text="Pattern:").pack(side='left', padx=5)
        self.redis_pattern = ttk.Entry(search_frame, width=20)
        self.redis_pattern.insert(0, "*")
        self.redis_pattern.pack(side='left', padx=5)

        ttk.Button(search_frame, text="Search", command=self.refresh_redis_tree).pack(side='left', padx=5)

        # Treeview for keys
        tree_scroll = ttk.Scrollbar(left_frame)
        tree_scroll.pack(side='right', fill='y')

        self.redis_tree = ttk.Treeview(left_frame, yscrollcommand=tree_scroll.set, selectmode='browse',
                                        columns=('type',), show='tree headings')
        self.redis_tree.pack(side='left', fill='both', expand=True)
        tree_scroll.config(command=self.redis_tree.yview)

        self.redis_tree.heading('#0', text='Key')
        self.redis_tree.heading('type', text='Type')
        self.redis_tree.column('type', width=80)
        self.redis_tree.bind('<<TreeviewSelect>>', self.on_redis_tree_select)

        # Right panel - Commands and Results
        right_frame = ttk.Frame(paned)
        paned.add(right_frame, weight=2)

        # Command Frame
        cmd_frame = ttk.LabelFrame(right_frame, text="Commands", padding=10)
        cmd_frame.pack(fill='both', expand=True)

        # Command controls
        ctrl_frame = ttk.Frame(cmd_frame)
        ctrl_frame.pack(fill='x', pady=5)

        ttk.Label(ctrl_frame, text="Command:").grid(row=0, column=0, sticky='w', padx=5, pady=5)
        self.redis_command = ttk.Combobox(ctrl_frame, width=15,
                                          values=["GET", "SET", "DEL", "KEYS", "HGET", "HGETALL",
                                                  "LRANGE", "SMEMBERS", "TTL", "INFO", "CUSTOM"])
        self.redis_command.set("GET")
        self.redis_command.grid(row=0, column=1, padx=5, pady=5, sticky='w')
        self.redis_command.bind("<<ComboboxSelected>>", self.on_redis_command_change)

        ttk.Label(ctrl_frame, text="Key:").grid(row=0, column=2, sticky='w', padx=5, pady=5)
        self.redis_key = ttk.Entry(ctrl_frame, width=40)
        self.redis_key.grid(row=0, column=3, padx=5, pady=5, sticky='ew')
        ctrl_frame.columnconfigure(3, weight=1)

        ttk.Label(cmd_frame, text="Value/Args:").pack(anchor='w', padx=5, pady=5)
        self.redis_value = scrolledtext.ScrolledText(cmd_frame, width=60, height=4)
        self.redis_value.pack(fill='x', padx=5, pady=5)

        ttk.Label(cmd_frame, text="Custom Command (JSON array):").pack(anchor='w', padx=5, pady=5)
        self.redis_custom = scrolledtext.ScrolledText(cmd_frame, width=60, height=3)
        self.redis_custom.insert('1.0', '["KEYS", "*"]')
        self.redis_custom.pack(fill='x', padx=5, pady=5)
        self.redis_custom.config(state='disabled')

        ttk.Button(cmd_frame, text="Execute", command=self.execute_redis_command).pack(pady=10)

        ttk.Label(cmd_frame, text="Results:").pack(anchor='w', padx=5, pady=5)
        self.redis_result = scrolledtext.ScrolledText(cmd_frame, width=80, height=15)
        self.redis_result.pack(fill='both', expand=True, padx=5, pady=5)

    def on_redis_command_change(self, event):
        cmd = self.redis_command.get()
        if cmd == "CUSTOM":
            self.redis_custom.config(state='normal')
        else:
            self.redis_custom.config(state='disabled')

    def connect_mongo(self):
        try:
            host = self.mongo_host.get()
            port = int(self.mongo_port.get())
            username = self.mongo_username.get()
            password = self.mongo_password.get()

            if username and password:
                uri = f"mongodb://{username}:{password}@{host}:{port}/"
            else:
                uri = f"mongodb://{host}:{port}/"

            self.mongo_client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            self.mongo_client.admin.command('ping')

            self.mongo_status.config(text="Status: Connected", foreground="green")
            messagebox.showinfo("Success", "Successfully connected to MongoDB!")
            self.refresh_mongo_tree()
        except Exception as e:
            self.mongo_status.config(text="Status: Connection Failed", foreground="red")
            messagebox.showerror("Connection Error", f"Failed to connect to MongoDB:\n{str(e)}")

    def refresh_mongo_tree(self):
        if not self.mongo_client:
            messagebox.showerror("Error", "Please connect to MongoDB first!")
            return

        try:
            # Clear existing tree
            for item in self.mongo_tree.get_children():
                self.mongo_tree.delete(item)

            # Get list of databases
            db_list = self.mongo_client.list_database_names()

            for db_name in db_list:
                # Insert database node
                db_node = self.mongo_tree.insert('', 'end', text=f"📁 {db_name}", tags=('database',))

                # Get collections for this database
                db = self.mongo_client[db_name]
                collections = db.list_collection_names()

                for coll_name in collections:
                    # Get collection stats
                    try:
                        stats = db.command("collStats", coll_name)
                        count = stats.get('count', 0)
                        self.mongo_tree.insert(db_node, 'end', text=f"📄 {coll_name} ({count} docs)",
                                               values=(db_name, coll_name), tags=('collection',))
                    except:
                        self.mongo_tree.insert(db_node, 'end', text=f"📄 {coll_name}",
                                               values=(db_name, coll_name), tags=('collection',))

            messagebox.showinfo("Success", f"Loaded {len(db_list)} databases")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to refresh database list:\n{str(e)}")

    def on_mongo_tree_select(self, event):
        selected = self.mongo_tree.selection()
        if not selected:
            return

        item = selected[0]
        tags = self.mongo_tree.item(item, 'tags')

        if 'collection' in tags:
            values = self.mongo_tree.item(item, 'values')
            if values and len(values) >= 2:
                db_name = values[0]
                coll_name = values[1]

                self.mongo_database.delete(0, 'end')
                self.mongo_database.insert(0, db_name)

                self.mongo_collection.delete(0, 'end')
                self.mongo_collection.insert(0, coll_name)

    def show_mongo_schema(self):
        if not self.mongo_client:
            messagebox.showerror("Error", "Please connect to MongoDB first!")
            return

        database = self.mongo_database.get()
        collection = self.mongo_collection.get()

        if not database or not collection:
            messagebox.showerror("Error", "Please select a database and collection!")
            return

        try:
            db = self.mongo_client[database]
            coll = db[collection]

            # Get sample documents to infer schema
            sample_docs = list(coll.find().limit(100))

            if not sample_docs:
                messagebox.showinfo("Schema Info", "Collection is empty")
                return

            # Analyze schema
            schema = {}
            for doc in sample_docs:
                for key, value in doc.items():
                    value_type = type(value).__name__
                    if key not in schema:
                        schema[key] = set()
                    schema[key].add(value_type)

            # Build schema report
            schema_report = {
                "database": database,
                "collection": collection,
                "document_count": coll.count_documents({}),
                "sample_size": len(sample_docs),
                "fields": {key: list(types) for key, types in schema.items()}
            }

            # Add sample document
            sample_doc = sample_docs[0].copy()
            if '_id' in sample_doc:
                sample_doc['_id'] = str(sample_doc['_id'])
            schema_report["sample_document"] = sample_doc

            result_text = json.dumps(schema_report, indent=2, ensure_ascii=False)

            self.mongo_result.delete('1.0', 'end')
            self.mongo_result.insert('1.0', result_text)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to analyze schema:\n{str(e)}")

    def disconnect_mongo(self):
        if self.mongo_client:
            self.mongo_client.close()
            self.mongo_client = None
            self.mongo_status.config(text="Status: Disconnected", foreground="red")
            messagebox.showinfo("Disconnected", "Disconnected from MongoDB")

    def execute_mongo_query(self):
        if not self.mongo_client:
            messagebox.showerror("Error", "Please connect to MongoDB first!")
            return

        try:
            database = self.mongo_database.get()
            collection = self.mongo_collection.get()
            query_str = self.mongo_query.get('1.0', 'end-1c')
            limit = int(self.mongo_limit.get())

            if not database or not collection:
                messagebox.showerror("Error", "Please specify database and collection!")
                return

            query = json.loads(query_str)

            db = self.mongo_client[database]
            coll = db[collection]

            results = list(coll.find(query).limit(limit))

            # Convert ObjectId to string for JSON serialization
            for doc in results:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])

            result_text = json.dumps(results, indent=2, ensure_ascii=False)

            self.mongo_result.delete('1.0', 'end')
            self.mongo_result.insert('1.0', result_text)

            messagebox.showinfo("Success", f"Found {len(results)} documents")
        except json.JSONDecodeError as e:
            messagebox.showerror("JSON Error", f"Invalid JSON query:\n{str(e)}")
        except Exception as e:
            messagebox.showerror("Query Error", f"Failed to execute query:\n{str(e)}")

    def connect_redis(self):
        try:
            host = self.redis_host.get()
            port = int(self.redis_port.get())
            password = self.redis_password.get() or None
            db = int(self.redis_db.get())

            self.redis_client = redis.Redis(
                host=host,
                port=port,
                password=password,
                db=db,
                decode_responses=True,
                socket_connect_timeout=5
            )

            self.redis_client.ping()

            self.redis_status.config(text="Status: Connected", foreground="green")
            messagebox.showinfo("Success", "Successfully connected to Redis!")
            self.refresh_redis_tree()
        except Exception as e:
            self.redis_status.config(text="Status: Connection Failed", foreground="red")
            messagebox.showerror("Connection Error", f"Failed to connect to Redis:\n{str(e)}")

    def refresh_redis_tree(self):
        if not self.redis_client:
            messagebox.showerror("Error", "Please connect to Redis first!")
            return

        try:
            # Clear existing tree
            for item in self.redis_tree.get_children():
                self.redis_tree.delete(item)

            # Get pattern
            pattern = self.redis_pattern.get() or "*"

            # Get keys matching pattern (limit to 1000 for performance)
            keys = self.redis_client.keys(pattern)
            if len(keys) > 1000:
                keys = keys[:1000]
                messagebox.showwarning("Warning", "Showing first 1000 keys only")

            # Group keys by prefix (split by ':')
            key_groups = defaultdict(list)
            for key in sorted(keys):
                if ':' in key:
                    prefix = key.split(':', 1)[0]
                    key_groups[prefix].append(key)
                else:
                    key_groups['_root'].append(key)

            # Add to tree
            for group, group_keys in sorted(key_groups.items()):
                if group == '_root':
                    # Add keys without prefix directly to root
                    for key in group_keys:
                        key_type = self.redis_client.type(key)
                        self.redis_tree.insert('', 'end', text=f"🔑 {key}",
                                               values=(key_type,), tags=('key',))
                else:
                    # Create group node
                    group_node = self.redis_tree.insert('', 'end', text=f"📂 {group}",
                                                         tags=('group',))

                    # Add keys in this group
                    for key in group_keys:
                        key_type = self.redis_client.type(key)
                        display_name = key.split(':', 1)[1] if ':' in key else key
                        self.redis_tree.insert(group_node, 'end', text=f"🔑 {display_name}",
                                               values=(key_type,), tags=('key', key))

            messagebox.showinfo("Success", f"Loaded {len(keys)} keys")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to refresh key list:\n{str(e)}")

    def on_redis_tree_select(self, event):
        selected = self.redis_tree.selection()
        if not selected:
            return

        item = selected[0]
        tags = self.redis_tree.item(item, 'tags')

        if 'key' in tags:
            # Find the actual key name from tags
            key_name = None
            for tag in tags:
                if tag != 'key':
                    key_name = tag
                    break

            # If not in tags, get from text
            if not key_name:
                text = self.redis_tree.item(item, 'text')
                key_name = text.replace('🔑 ', '')

            # Set key in entry
            self.redis_key.delete(0, 'end')
            self.redis_key.insert(0, key_name)

            # Auto-fetch value based on type
            try:
                key_type = self.redis_client.type(key_name)
                result = None

                if key_type == 'string':
                    result = self.redis_client.get(key_name)
                    self.redis_command.set("GET")
                elif key_type == 'hash':
                    result = self.redis_client.hgetall(key_name)
                    self.redis_command.set("HGETALL")
                elif key_type == 'list':
                    result = self.redis_client.lrange(key_name, 0, -1)
                    self.redis_command.set("LRANGE")
                elif key_type == 'set':
                    result = self.redis_client.smembers(key_name)
                    self.redis_command.set("SMEMBERS")
                elif key_type == 'zset':
                    result = self.redis_client.zrange(key_name, 0, -1, withscores=True)
                    self.redis_command.set("CUSTOM")

                if result is not None:
                    result_text = json.dumps(result, indent=2, ensure_ascii=False, default=str)
                    self.redis_result.delete('1.0', 'end')
                    self.redis_result.insert('1.0', result_text)

            except Exception as e:
                self.redis_result.delete('1.0', 'end')
                self.redis_result.insert('1.0', f"Error fetching value: {str(e)}")

    def disconnect_redis(self):
        if self.redis_client:
            self.redis_client.close()
            self.redis_client = None
            self.redis_status.config(text="Status: Disconnected", foreground="red")
            messagebox.showinfo("Disconnected", "Disconnected from Redis")

    def execute_redis_command(self):
        if not self.redis_client:
            messagebox.showerror("Error", "Please connect to Redis first!")
            return

        try:
            cmd = self.redis_command.get()
            key = self.redis_key.get()
            value = self.redis_value.get('1.0', 'end-1c').strip()

            result = None

            if cmd == "GET":
                result = self.redis_client.get(key)
            elif cmd == "SET":
                result = self.redis_client.set(key, value)
            elif cmd == "DEL":
                result = self.redis_client.delete(key)
            elif cmd == "KEYS":
                pattern = key if key else "*"
                result = self.redis_client.keys(pattern)
            elif cmd == "HGET":
                field = value
                result = self.redis_client.hget(key, field)
            elif cmd == "HGETALL":
                result = self.redis_client.hgetall(key)
            elif cmd == "LRANGE":
                parts = value.split()
                start = int(parts[0]) if len(parts) > 0 else 0
                end = int(parts[1]) if len(parts) > 1 else -1
                result = self.redis_client.lrange(key, start, end)
            elif cmd == "SMEMBERS":
                result = self.redis_client.smembers(key)
            elif cmd == "TTL":
                result = self.redis_client.ttl(key)
            elif cmd == "INFO":
                section = key if key else None
                result = self.redis_client.info(section)
            elif cmd == "CUSTOM":
                custom_cmd = self.redis_custom.get('1.0', 'end-1c').strip()
                cmd_list = json.loads(custom_cmd)
                result = self.redis_client.execute_command(*cmd_list)

            result_text = json.dumps(result, indent=2, ensure_ascii=False, default=str)

            self.redis_result.delete('1.0', 'end')
            self.redis_result.insert('1.0', result_text)

            messagebox.showinfo("Success", "Command executed successfully")
        except json.JSONDecodeError as e:
            messagebox.showerror("JSON Error", f"Invalid JSON in custom command:\n{str(e)}")
        except Exception as e:
            messagebox.showerror("Command Error", f"Failed to execute command:\n{str(e)}")


if __name__ == "__main__":
    root = tk.Tk()
    app = DatabaseQueryTool(root)
    root.mainloop()
