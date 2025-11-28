import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog, simpledialog
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import redis
import json
import csv
from collections import defaultdict
from datetime import datetime
from config_manager import ConfigManager
import time
import re


class JsonHighlightText(scrolledtext.ScrolledText):
    """Text widget with JSON syntax highlighting"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Define tags for syntax highlighting
        self.tag_config('string', foreground='#0451a5')
        self.tag_config('number', foreground='#098658')
        self.tag_config('boolean', foreground='#0000ff')
        self.tag_config('null', foreground='#0000ff')
        self.tag_config('key', foreground='#a31515')
        self.tag_config('brace', foreground='#000000')

    def highlight(self):
        """Apply JSON syntax highlighting"""
        content = self.get('1.0', 'end-1c')

        # Remove all tags
        for tag in ('string', 'number', 'boolean', 'null', 'key', 'brace'):
            self.tag_remove(tag, '1.0', 'end')

        # String pattern
        for match in re.finditer(r'"([^"\\]|\\.)*"', content):
            start = f"1.0+{match.start()}c"
            end = f"1.0+{match.end()}c"
            # Check if it's a key (followed by :)
            next_char_idx = match.end()
            is_key = next_char_idx < len(content) and content[next_char_idx:].lstrip().startswith(':')
            self.tag_add('key' if is_key else 'string', start, end)

        # Number pattern
        for match in re.finditer(r'\b-?\d+\.?\d*\b', content):
            start = f"1.0+{match.start()}c"
            end = f"1.0+{match.end()}c"
            self.tag_add('number', start, end)

        # Boolean pattern
        for match in re.finditer(r'\b(true|false)\b', content):
            start = f"1.0+{match.start()}c"
            end = f"1.0+{match.end()}c"
            self.tag_add('boolean', start, end)

        # Null pattern
        for match in re.finditer(r'\bnull\b', content):
            start = f"1.0+{match.start()}c"
            end = f"1.0+{match.end()}c"
            self.tag_add('null', start, end)


class MongoDocumentEditor(tk.Toplevel):
    """Dialog for editing MongoDB documents"""

    def __init__(self, parent, document=None, mode='edit'):
        super().__init__(parent)
        self.title(f"{mode.title()} Document")
        self.geometry("600x500")
        self.result = None

        # Document editor
        ttk.Label(self, text="Document (JSON):").pack(anchor='w', padx=10, pady=5)
        self.doc_text = JsonHighlightText(self, width=70, height=25)
        self.doc_text.pack(fill='both', expand=True, padx=10, pady=5)

        if document:
            # Convert ObjectId to string for editing
            if '_id' in document:
                document['_id'] = str(document['_id'])
            self.doc_text.insert('1.0', json.dumps(document, indent=2, ensure_ascii=False))
        else:
            self.doc_text.insert('1.0', '{\n  \n}')

        self.doc_text.highlight()

        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)

        ttk.Button(btn_frame, text="Save", command=self.save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Format JSON", command=self.format_json).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Cancel", command=self.destroy).pack(side='right', padx=5)

        self.transient(parent)
        self.grab_set()

    def format_json(self):
        """Format JSON content"""
        try:
            content = self.doc_text.get('1.0', 'end-1c')
            doc = json.loads(content)
            formatted = json.dumps(doc, indent=2, ensure_ascii=False)
            self.doc_text.delete('1.0', 'end')
            self.doc_text.insert('1.0', formatted)
            self.doc_text.highlight()
        except json.JSONDecodeError as e:
            messagebox.showerror("JSON Error", f"Invalid JSON: {str(e)}")

    def save(self):
        """Save document"""
        try:
            content = self.doc_text.get('1.0', 'end-1c')
            self.result = json.loads(content)
            self.destroy()
        except json.JSONDecodeError as e:
            messagebox.showerror("JSON Error", f"Invalid JSON: {str(e)}")


class RedisValueEditor(tk.Toplevel):
    """Dialog for editing Redis values"""

    def __init__(self, parent, key='', value='', value_type='string'):
        super().__init__(parent)
        self.title(f"Edit Redis Value ({value_type})")
        self.geometry("600x400")
        self.result = None

        # Key
        ttk.Label(self, text="Key:").pack(anchor='w', padx=10, pady=5)
        self.key_entry = ttk.Entry(self, width=70)
        self.key_entry.insert(0, key)
        self.key_entry.pack(fill='x', padx=10, pady=5)

        # Value
        ttk.Label(self, text="Value:").pack(anchor='w', padx=10, pady=5)
        self.value_text = scrolledtext.ScrolledText(self, width=70, height=15)
        if isinstance(value, dict) or isinstance(value, list):
            self.value_text.insert('1.0', json.dumps(value, indent=2, ensure_ascii=False))
        else:
            self.value_text.insert('1.0', str(value))
        self.value_text.pack(fill='both', expand=True, padx=10, pady=5)

        # TTL
        ttl_frame = ttk.Frame(self)
        ttl_frame.pack(fill='x', padx=10, pady=5)

        ttk.Label(ttl_frame, text="TTL (seconds, -1 for no expiry):").pack(side='left', padx=5)
        self.ttl_entry = ttk.Entry(ttl_frame, width=10)
        self.ttl_entry.insert(0, "-1")
        self.ttl_entry.pack(side='left', padx=5)

        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)

        ttk.Button(btn_frame, text="Save", command=self.save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Cancel", command=self.destroy).pack(side='right', padx=5)

        self.transient(parent)
        self.grab_set()

    def save(self):
        """Save value"""
        key = self.key_entry.get()
        value = self.value_text.get('1.0', 'end-1c')
        ttl = self.ttl_entry.get()

        if not key:
            messagebox.showerror("Error", "Key is required")
            return

        try:
            ttl_int = int(ttl)
        except ValueError:
            messagebox.showerror("Error", "TTL must be a number")
            return

        self.result = {
            'key': key,
            'value': value,
            'ttl': ttl_int
        }
        self.destroy()


class ProfileManager(tk.Toplevel):
    """Dialog for managing connection profiles"""

    def __init__(self, parent, config_manager, db_type='mongo'):
        super().__init__(parent)
        self.config_manager = config_manager
        self.db_type = db_type
        self.title(f"{db_type.upper()} Connection Profiles")
        self.geometry("700x500")

        # Profile list
        list_frame = ttk.LabelFrame(self, text="Saved Profiles", padding=10)
        list_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Treeview
        if db_type == 'mongo':
            columns = ('name', 'host', 'port', 'username', 'database')
        else:
            columns = ('name', 'host', 'port', 'db')

        self.tree = ttk.Treeview(list_frame, columns=columns, show='headings')

        for col in columns:
            self.tree.heading(col, text=col.title())
            self.tree.column(col, width=100)

        self.tree.pack(fill='both', expand=True, side='left')

        scrollbar = ttk.Scrollbar(list_frame, orient='vertical', command=self.tree.yview)
        scrollbar.pack(side='right', fill='y')
        self.tree.configure(yscrollcommand=scrollbar.set)

        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)

        ttk.Button(btn_frame, text="Add New", command=self.add_profile).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Delete", command=self.delete_profile).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Close", command=self.destroy).pack(side='right', padx=5)

        self.refresh_list()

    def refresh_list(self):
        """Refresh profile list"""
        self.tree.delete(*self.tree.get_children())

        if self.db_type == 'mongo':
            profiles = self.config_manager.get_mongo_profiles()
            for profile in profiles:
                self.tree.insert('', 'end', values=(
                    profile['name'],
                    profile['host'],
                    profile['port'],
                    profile['username'],
                    profile.get('database', '')
                ))
        else:
            profiles = self.config_manager.get_redis_profiles()
            for profile in profiles:
                self.tree.insert('', 'end', values=(
                    profile['name'],
                    profile['host'],
                    profile['port'],
                    profile['db']
                ))

    def add_profile(self):
        """Add new profile"""
        dialog = ProfileEditDialog(self, self.config_manager, self.db_type)
        self.wait_window(dialog)
        self.refresh_list()

    def delete_profile(self):
        """Delete selected profile"""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Please select a profile")
            return

        item = self.tree.item(selected[0])
        profile_name = item['values'][0]

        if messagebox.askyesno("Confirm", f"Delete profile '{profile_name}'?"):
            if self.db_type == 'mongo':
                self.config_manager.delete_mongo_profile(profile_name)
            else:
                self.config_manager.delete_redis_profile(profile_name)
            self.refresh_list()


class ProfileEditDialog(tk.Toplevel):
    """Dialog for editing profile details"""

    def __init__(self, parent, config_manager, db_type='mongo', profile=None):
        super().__init__(parent)
        self.config_manager = config_manager
        self.db_type = db_type
        self.title("Add Profile" if not profile else "Edit Profile")
        self.geometry("400x400")

        # Form
        form_frame = ttk.Frame(self, padding=10)
        form_frame.pack(fill='both', expand=True)

        row = 0

        # Profile Name
        ttk.Label(form_frame, text="Profile Name:").grid(row=row, column=0, sticky='w', pady=5)
        self.name_entry = ttk.Entry(form_frame, width=30)
        self.name_entry.grid(row=row, column=1, sticky='ew', pady=5)
        row += 1

        # Host
        ttk.Label(form_frame, text="Host:").grid(row=row, column=0, sticky='w', pady=5)
        self.host_entry = ttk.Entry(form_frame, width=30)
        self.host_entry.insert(0, "localhost")
        self.host_entry.grid(row=row, column=1, sticky='ew', pady=5)
        row += 1

        # Port
        ttk.Label(form_frame, text="Port:").grid(row=row, column=0, sticky='w', pady=5)
        self.port_entry = ttk.Entry(form_frame, width=30)
        self.port_entry.insert(0, "27017" if db_type == 'mongo' else "6379")
        self.port_entry.grid(row=row, column=1, sticky='ew', pady=5)
        row += 1

        if db_type == 'mongo':
            # Username
            ttk.Label(form_frame, text="Username:").grid(row=row, column=0, sticky='w', pady=5)
            self.username_entry = ttk.Entry(form_frame, width=30)
            self.username_entry.grid(row=row, column=1, sticky='ew', pady=5)
            row += 1

            # Password
            ttk.Label(form_frame, text="Password:").grid(row=row, column=0, sticky='w', pady=5)
            self.password_entry = ttk.Entry(form_frame, width=30, show='*')
            self.password_entry.grid(row=row, column=1, sticky='ew', pady=5)
            row += 1

            # Database
            ttk.Label(form_frame, text="Database:").grid(row=row, column=0, sticky='w', pady=5)
            self.database_entry = ttk.Entry(form_frame, width=30)
            self.database_entry.grid(row=row, column=1, sticky='ew', pady=5)
            row += 1
        else:
            # Password
            ttk.Label(form_frame, text="Password:").grid(row=row, column=0, sticky='w', pady=5)
            self.password_entry = ttk.Entry(form_frame, width=30, show='*')
            self.password_entry.grid(row=row, column=1, sticky='ew', pady=5)
            row += 1

            # DB Number
            ttk.Label(form_frame, text="DB:").grid(row=row, column=0, sticky='w', pady=5)
            self.db_entry = ttk.Entry(form_frame, width=30)
            self.db_entry.insert(0, "0")
            self.db_entry.grid(row=row, column=1, sticky='ew', pady=5)
            row += 1

        form_frame.columnconfigure(1, weight=1)

        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)

        ttk.Button(btn_frame, text="Save", command=self.save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Cancel", command=self.destroy).pack(side='right', padx=5)

        self.transient(parent)
        self.grab_set()

    def save(self):
        """Save profile"""
        name = self.name_entry.get().strip()
        host = self.host_entry.get().strip()
        port_str = self.port_entry.get().strip()

        if not name or not host or not port_str:
            messagebox.showerror("Error", "Name, host, and port are required")
            return

        try:
            port = int(port_str)
        except ValueError:
            messagebox.showerror("Error", "Port must be a number")
            return

        if self.db_type == 'mongo':
            username = self.username_entry.get().strip()
            password = self.password_entry.get()
            database = self.database_entry.get().strip()

            self.config_manager.add_mongo_profile(
                name=name,
                host=host,
                port=port,
                username=username,
                password=password,
                database=database
            )
        else:
            password = self.password_entry.get()
            db_str = self.db_entry.get().strip()

            try:
                db = int(db_str)
            except ValueError:
                messagebox.showerror("Error", "DB must be a number")
                return

            self.config_manager.add_redis_profile(
                name=name,
                host=host,
                port=port,
                password=password,
                db=db
            )

        messagebox.showinfo("Success", "Profile saved successfully")
        self.destroy()


class DatabaseQueryTool:
    def __init__(self, root):
        self.root = root
        self.root.title("MongoDB & Redis Query Tool - Advanced")
        self.root.geometry("1600x900")

        self.mongo_client = None
        self.redis_client = None
        self.config_manager = ConfigManager()
        self.auto_refresh_job = None

        self.setup_ui()
        self.apply_theme()

    def setup_ui(self):
        # Menu bar
        self.setup_menu()

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

        # Status bar
        self.status_bar = ttk.Label(self.root, text="Ready", relief='sunken')
        self.status_bar.pack(side='bottom', fill='x')

    def setup_menu(self):
        """Setup menu bar"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Export Results...", command=self.export_results)
        file_menu.add_command(label="Import Data...", command=self.import_data)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)

        # Connection menu
        conn_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Connection", menu=conn_menu)
        conn_menu.add_command(label="MongoDB Profiles...", command=self.manage_mongo_profiles)
        conn_menu.add_command(label="Redis Profiles...", command=self.manage_redis_profiles)

        # View menu
        view_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="View", menu=view_menu)
        view_menu.add_command(label="Toggle Theme", command=self.toggle_theme)
        view_menu.add_command(label="Clear Results", command=self.clear_results)

        # Tools menu
        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Tools", menu=tools_menu)
        tools_menu.add_command(label="Query History...", command=self.show_history)
        tools_menu.add_command(label="Favorites...", command=self.show_favorites)
        tools_menu.add_separator()
        tools_menu.add_command(label="Settings...", command=self.show_settings)

        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self.show_about)

    def setup_mongo_tab(self):
        # Connection Frame
        conn_frame = ttk.LabelFrame(self.mongo_frame, text="MongoDB Connection", padding=10)
        conn_frame.pack(fill='x', padx=10, pady=10)

        # Profile selector
        ttk.Label(conn_frame, text="Profile:").grid(row=0, column=0, sticky='w', padx=5, pady=5)
        self.mongo_profile_var = tk.StringVar()
        self.mongo_profile_combo = ttk.Combobox(conn_frame, textvariable=self.mongo_profile_var, width=20)
        self.mongo_profile_combo.grid(row=0, column=1, padx=5, pady=5)
        self.mongo_profile_combo.bind('<<ComboboxSelected>>', self.on_mongo_profile_select)
        self.update_mongo_profiles()

        ttk.Label(conn_frame, text="Host:").grid(row=0, column=2, sticky='w', padx=5, pady=5)
        self.mongo_host = ttk.Entry(conn_frame, width=15)
        self.mongo_host.insert(0, "localhost")
        self.mongo_host.grid(row=0, column=3, padx=5, pady=5)

        ttk.Label(conn_frame, text="Port:").grid(row=0, column=4, sticky='w', padx=5, pady=5)
        self.mongo_port = ttk.Entry(conn_frame, width=8)
        self.mongo_port.insert(0, "27017")
        self.mongo_port.grid(row=0, column=5, padx=5, pady=5)

        ttk.Label(conn_frame, text="User:").grid(row=0, column=6, sticky='w', padx=5, pady=5)
        self.mongo_username = ttk.Entry(conn_frame, width=12)
        self.mongo_username.grid(row=0, column=7, padx=5, pady=5)

        ttk.Label(conn_frame, text="Pass:").grid(row=0, column=8, sticky='w', padx=5, pady=5)
        self.mongo_password = ttk.Entry(conn_frame, width=12, show="*")
        self.mongo_password.grid(row=0, column=9, padx=5, pady=5)

        ttk.Button(conn_frame, text="Connect", command=self.connect_mongo).grid(row=0, column=10, padx=5, pady=5)
        ttk.Button(conn_frame, text="Save Profile", command=self.save_mongo_profile).grid(row=0, column=11, padx=5, pady=5)

        self.mongo_status = ttk.Label(conn_frame, text="Status: Disconnected", foreground="red")
        self.mongo_status.grid(row=1, column=0, columnspan=12, pady=5)

        # Main content with paned window
        content_frame = ttk.Frame(self.mongo_frame)
        content_frame.pack(fill='both', expand=True, padx=10, pady=10)

        paned = ttk.PanedWindow(content_frame, orient='horizontal')
        paned.pack(fill='both', expand=True)

        # Left panel - Database Browser
        left_frame = ttk.Frame(paned)
        paned.add(left_frame, weight=1)

        browser_header = ttk.Frame(left_frame)
        browser_header.pack(fill='x', pady=5)

        ttk.Label(browser_header, text="Database Browser", font=('Arial', 10, 'bold')).pack(side='left')
        ttk.Button(browser_header, text="Refresh", command=self.refresh_mongo_tree, width=10).pack(side='right', padx=2)

        tree_scroll = ttk.Scrollbar(left_frame)
        tree_scroll.pack(side='right', fill='y')

        self.mongo_tree = ttk.Treeview(left_frame, yscrollcommand=tree_scroll.set, selectmode='browse')
        self.mongo_tree.pack(side='left', fill='both', expand=True)
        tree_scroll.config(command=self.mongo_tree.yview)

        self.mongo_tree.heading('#0', text='Databases & Collections')
        self.mongo_tree.bind('<<TreeviewSelect>>', self.on_mongo_tree_select)
        self.mongo_tree.bind('<Double-1>', self.on_mongo_tree_double_click)

        # Right panel - Query and Results with tabs
        right_frame = ttk.Frame(paned)
        paned.add(right_frame, weight=3)

        # Sub-notebook for query tabs
        self.mongo_query_notebook = ttk.Notebook(right_frame)
        self.mongo_query_notebook.pack(fill='both', expand=True)

        # Create first query tab
        self.mongo_query_tabs = []
        self.add_mongo_query_tab()

        # Tab management buttons
        tab_btn_frame = ttk.Frame(right_frame)
        tab_btn_frame.pack(fill='x', pady=5)
        ttk.Button(tab_btn_frame, text="+ New Tab", command=self.add_mongo_query_tab).pack(side='left', padx=5)
        ttk.Button(tab_btn_frame, text="Close Tab", command=self.close_mongo_query_tab).pack(side='left', padx=5)

    def add_mongo_query_tab(self, title=None):
        """Add a new query tab for MongoDB"""
        if title is None:
            title = f"Query {len(self.mongo_query_tabs) + 1}"

        tab_frame = ttk.Frame(self.mongo_query_notebook)
        self.mongo_query_notebook.add(tab_frame, text=title)

        # Query controls
        ctrl_frame = ttk.Frame(tab_frame)
        ctrl_frame.pack(fill='x', pady=5)

        ttk.Label(ctrl_frame, text="DB:").grid(row=0, column=0, sticky='w', padx=5)
        db_entry = ttk.Entry(ctrl_frame, width=20)
        db_entry.grid(row=0, column=1, padx=5)

        ttk.Label(ctrl_frame, text="Collection:").grid(row=0, column=2, sticky='w', padx=5)
        coll_entry = ttk.Entry(ctrl_frame, width=20)
        coll_entry.grid(row=0, column=3, padx=5)

        ttk.Button(ctrl_frame, text="Schema", command=lambda: self.show_mongo_schema(db_entry, coll_entry, result_text)).grid(row=0, column=4, padx=5)
        ttk.Button(ctrl_frame, text="Indexes", command=lambda: self.show_mongo_indexes(db_entry, coll_entry, result_text)).grid(row=0, column=5, padx=5)
        ttk.Button(ctrl_frame, text="Stats", command=lambda: self.show_mongo_stats(db_entry, coll_entry, result_text)).grid(row=0, column=6, padx=5)

        # Query type selector
        query_type_frame = ttk.Frame(tab_frame)
        query_type_frame.pack(fill='x', pady=5)

        ttk.Label(query_type_frame, text="Query Type:").pack(side='left', padx=5)
        query_type_var = tk.StringVar(value="find")
        ttk.Radiobutton(query_type_frame, text="Find", variable=query_type_var, value="find").pack(side='left', padx=5)
        ttk.Radiobutton(query_type_frame, text="Aggregate", variable=query_type_var, value="aggregate").pack(side='left', padx=5)
        ttk.Radiobutton(query_type_frame, text="Count", variable=query_type_var, value="count").pack(side='left', padx=5)

        # Query input
        ttk.Label(tab_frame, text="Query (JSON):").pack(anchor='w', padx=5, pady=5)
        query_text = JsonHighlightText(tab_frame, width=60, height=8)
        query_text.insert('1.0', '{}')
        query_text.pack(fill='x', padx=5, pady=5)
        query_text.bind('<KeyRelease>', lambda e: query_text.highlight())

        # Options
        opt_frame = ttk.Frame(tab_frame)
        opt_frame.pack(fill='x', pady=5)

        ttk.Label(opt_frame, text="Limit:").pack(side='left', padx=5)
        limit_entry = ttk.Entry(opt_frame, width=10)
        limit_entry.insert(0, "100")
        limit_entry.pack(side='left', padx=5)

        ttk.Label(opt_frame, text="Skip:").pack(side='left', padx=5)
        skip_entry = ttk.Entry(opt_frame, width=10)
        skip_entry.insert(0, "0")
        skip_entry.pack(side='left', padx=5)

        ttk.Button(opt_frame, text="Execute", command=lambda: self.execute_mongo_query_tab(
            db_entry, coll_entry, query_text, query_type_var, limit_entry, skip_entry, result_text, time_label
        )).pack(side='left', padx=10)

        ttk.Button(opt_frame, text="Add to Favorites", command=lambda: self.add_mongo_favorite(
            db_entry, coll_entry, query_text
        )).pack(side='left', padx=5)

        time_label = ttk.Label(opt_frame, text="")
        time_label.pack(side='right', padx=5)

        # Results
        ttk.Label(tab_frame, text="Results:").pack(anchor='w', padx=5, pady=5)

        result_frame = ttk.Frame(tab_frame)
        result_frame.pack(fill='both', expand=True, padx=5, pady=5)

        # View mode selector
        view_mode_frame = ttk.Frame(result_frame)
        view_mode_frame.pack(fill='x', pady=2)

        view_mode_var = tk.StringVar(value="json")
        ttk.Radiobutton(view_mode_frame, text="JSON View", variable=view_mode_var, value="json",
                       command=lambda: self.switch_result_view(result_text, table_frame, view_mode_var)).pack(side='left', padx=5)
        ttk.Radiobutton(view_mode_frame, text="Table View", variable=view_mode_var, value="table",
                       command=lambda: self.switch_result_view(result_text, table_frame, view_mode_var)).pack(side='left', padx=5)

        # JSON result view
        result_text = JsonHighlightText(result_frame, width=80, height=20)
        result_text.pack(fill='both', expand=True)

        # Table view (hidden by default)
        table_frame = ttk.Frame(result_frame)

        # Store references
        tab_data = {
            'frame': tab_frame,
            'db_entry': db_entry,
            'coll_entry': coll_entry,
            'query_text': query_text,
            'query_type_var': query_type_var,
            'limit_entry': limit_entry,
            'skip_entry': skip_entry,
            'result_text': result_text,
            'time_label': time_label,
            'view_mode_var': view_mode_var,
            'table_frame': table_frame
        }

        self.mongo_query_tabs.append(tab_data)
        self.mongo_query_notebook.select(tab_frame)

        return tab_data

    def close_mongo_query_tab(self):
        """Close current MongoDB query tab"""
        if len(self.mongo_query_tabs) <= 1:
            messagebox.showwarning("Warning", "Cannot close the last tab")
            return

        current_tab = self.mongo_query_notebook.select()
        current_index = self.mongo_query_notebook.index(current_tab)

        self.mongo_query_notebook.forget(current_tab)
        del self.mongo_query_tabs[current_index]

    def switch_result_view(self, result_text, table_frame, view_mode_var):
        """Switch between JSON and Table view"""
        mode = view_mode_var.get()

        if mode == "json":
            table_frame.pack_forget()
            result_text.pack(fill='both', expand=True)
        else:
            result_text.pack_forget()
            table_frame.pack(fill='both', expand=True)

    def execute_mongo_query_tab(self, db_entry, coll_entry, query_text, query_type_var,
                                limit_entry, skip_entry, result_text, time_label):
        """Execute MongoDB query from tab"""
        if not self.mongo_client:
            messagebox.showerror("Error", "Please connect to MongoDB first!")
            return

        database = db_entry.get().strip()
        collection = coll_entry.get().strip()
        query_str = query_text.get('1.0', 'end-1c')
        query_type = query_type_var.get()

        if not database or not collection:
            messagebox.showerror("Error", "Please specify database and collection!")
            return

        try:
            limit = int(limit_entry.get())
            skip = int(skip_entry.get())
        except ValueError:
            messagebox.showerror("Error", "Limit and Skip must be numbers")
            return

        try:
            query = json.loads(query_str)

            db = self.mongo_client[database]
            coll = db[collection]

            start_time = time.time()

            if query_type == "find":
                results = list(coll.find(query).skip(skip).limit(limit))
            elif query_type == "aggregate":
                if isinstance(query, list):
                    pipeline = query
                else:
                    pipeline = [query]
                results = list(coll.aggregate(pipeline))
            elif query_type == "count":
                count = coll.count_documents(query)
                results = [{"count": count}]
            else:
                results = []

            execution_time = time.time() - start_time

            # Convert ObjectId to string
            for doc in results:
                if '_id' in doc:
                    doc['_id'] = str(doc['_id'])

            result_text.delete('1.0', 'end')
            result_json = json.dumps(results, indent=2, ensure_ascii=False)
            result_text.insert('1.0', result_json)
            result_text.highlight()

            time_label.config(text=f"Time: {execution_time:.3f}s | Results: {len(results)}")

            # Add to history
            self.config_manager.add_to_history(
                'mongo', query_str, database, collection, execution_time
            )

            self.status_bar.config(text=f"Query executed successfully: {len(results)} documents in {execution_time:.3f}s")

        except json.JSONDecodeError as e:
            messagebox.showerror("JSON Error", f"Invalid JSON query:\n{str(e)}")
        except Exception as e:
            messagebox.showerror("Query Error", f"Failed to execute query:\n{str(e)}")
            time_label.config(text="Error")

    def setup_redis_tab(self):
        # Connection Frame
        conn_frame = ttk.LabelFrame(self.redis_frame, text="Redis Connection", padding=10)
        conn_frame.pack(fill='x', padx=10, pady=10)

        # Profile selector
        ttk.Label(conn_frame, text="Profile:").grid(row=0, column=0, sticky='w', padx=5, pady=5)
        self.redis_profile_var = tk.StringVar()
        self.redis_profile_combo = ttk.Combobox(conn_frame, textvariable=self.redis_profile_var, width=20)
        self.redis_profile_combo.grid(row=0, column=1, padx=5, pady=5)
        self.redis_profile_combo.bind('<<ComboboxSelected>>', self.on_redis_profile_select)
        self.update_redis_profiles()

        ttk.Label(conn_frame, text="Host:").grid(row=0, column=2, sticky='w', padx=5, pady=5)
        self.redis_host = ttk.Entry(conn_frame, width=15)
        self.redis_host.insert(0, "localhost")
        self.redis_host.grid(row=0, column=3, padx=5, pady=5)

        ttk.Label(conn_frame, text="Port:").grid(row=0, column=4, sticky='w', padx=5, pady=5)
        self.redis_port = ttk.Entry(conn_frame, width=8)
        self.redis_port.insert(0, "6379")
        self.redis_port.grid(row=0, column=5, padx=5, pady=5)

        ttk.Label(conn_frame, text="Pass:").grid(row=0, column=6, sticky='w', padx=5, pady=5)
        self.redis_password = ttk.Entry(conn_frame, width=12, show="*")
        self.redis_password.grid(row=0, column=7, padx=5, pady=5)

        ttk.Label(conn_frame, text="DB:").grid(row=0, column=8, sticky='w', padx=5, pady=5)
        self.redis_db = ttk.Entry(conn_frame, width=8)
        self.redis_db.insert(0, "0")
        self.redis_db.grid(row=0, column=9, padx=5, pady=5)

        ttk.Button(conn_frame, text="Connect", command=self.connect_redis).grid(row=0, column=10, padx=5, pady=5)
        ttk.Button(conn_frame, text="Save Profile", command=self.save_redis_profile).grid(row=0, column=11, padx=5, pady=5)

        self.redis_status = ttk.Label(conn_frame, text="Status: Disconnected", foreground="red")
        self.redis_status.grid(row=1, column=0, columnspan=12, pady=5)

        # Main content with paned window
        content_frame = ttk.Frame(self.redis_frame)
        content_frame.pack(fill='both', expand=True, padx=10, pady=10)

        paned = ttk.PanedWindow(content_frame, orient='horizontal')
        paned.pack(fill='both', expand=True)

        # Left panel - Key Browser
        left_frame = ttk.Frame(paned)
        paned.add(left_frame, weight=1)

        browser_header = ttk.Frame(left_frame)
        browser_header.pack(fill='x', pady=5)

        ttk.Label(browser_header, text="Key Browser", font=('Arial', 10, 'bold')).pack(side='left')

        # Search controls
        search_frame = ttk.Frame(left_frame)
        search_frame.pack(fill='x', pady=5)

        ttk.Label(search_frame, text="Pattern:").pack(side='left', padx=5)
        self.redis_pattern = ttk.Entry(search_frame, width=15)
        self.redis_pattern.insert(0, "*")
        self.redis_pattern.pack(side='left', padx=5)

        ttk.Button(search_frame, text="Search", command=self.refresh_redis_tree, width=8).pack(side='left', padx=2)

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
        self.redis_tree.bind('<Double-1>', self.on_redis_tree_double_click)

        # Right panel - Commands and Results
        right_frame = ttk.Frame(paned)
        paned.add(right_frame, weight=3)

        cmd_frame = ttk.LabelFrame(right_frame, text="Commands", padding=10)
        cmd_frame.pack(fill='both', expand=True)

        # Command controls
        ctrl_frame = ttk.Frame(cmd_frame)
        ctrl_frame.pack(fill='x', pady=5)

        ttk.Label(ctrl_frame, text="Command:").grid(row=0, column=0, sticky='w', padx=5, pady=5)
        self.redis_command = ttk.Combobox(ctrl_frame, width=12,
                                          values=["GET", "SET", "DEL", "KEYS", "HGET", "HGETALL",
                                                  "LRANGE", "SMEMBERS", "TTL", "INFO", "DBSIZE", "CUSTOM"])
        self.redis_command.set("GET")
        self.redis_command.grid(row=0, column=1, padx=5, pady=5, sticky='w')
        self.redis_command.bind("<<ComboboxSelected>>", self.on_redis_command_change)

        ttk.Label(ctrl_frame, text="Key:").grid(row=0, column=2, sticky='w', padx=5, pady=5)
        self.redis_key = ttk.Entry(ctrl_frame, width=40)
        self.redis_key.grid(row=0, column=3, padx=5, pady=5, sticky='ew')

        ttk.Button(ctrl_frame, text="Edit", command=self.edit_redis_value).grid(row=0, column=4, padx=5, pady=5)
        ttk.Button(ctrl_frame, text="Delete", command=self.delete_redis_key).grid(row=0, column=5, padx=5, pady=5)

        ctrl_frame.columnconfigure(3, weight=1)

        ttk.Label(cmd_frame, text="Value/Args:").pack(anchor='w', padx=5, pady=5)
        self.redis_value = scrolledtext.ScrolledText(cmd_frame, width=60, height=4)
        self.redis_value.pack(fill='x', padx=5, pady=5)

        ttk.Label(cmd_frame, text="Custom Command (JSON array):").pack(anchor='w', padx=5, pady=5)
        self.redis_custom = scrolledtext.ScrolledText(cmd_frame, width=60, height=3)
        self.redis_custom.insert('1.0', '["KEYS", "*"]')
        self.redis_custom.pack(fill='x', padx=5, pady=5)
        self.redis_custom.config(state='disabled')

        exec_frame = ttk.Frame(cmd_frame)
        exec_frame.pack(fill='x', pady=5)

        ttk.Button(exec_frame, text="Execute", command=self.execute_redis_command).pack(side='left', padx=5)
        ttk.Button(exec_frame, text="Add to Favorites", command=self.add_redis_favorite).pack(side='left', padx=5)

        self.redis_time_label = ttk.Label(exec_frame, text="")
        self.redis_time_label.pack(side='right', padx=5)

        ttk.Label(cmd_frame, text="Results:").pack(anchor='w', padx=5, pady=5)
        self.redis_result = JsonHighlightText(cmd_frame, width=80, height=15)
        self.redis_result.pack(fill='both', expand=True, padx=5, pady=5)

    # MongoDB Methods
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
            self.status_bar.config(text="Connected to MongoDB")
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
            for item in self.mongo_tree.get_children():
                self.mongo_tree.delete(item)

            db_list = self.mongo_client.list_database_names()

            for db_name in db_list:
                db_node = self.mongo_tree.insert('', 'end', text=f"📁 {db_name}", tags=('database',))

                db = self.mongo_client[db_name]
                collections = db.list_collection_names()

                for coll_name in collections:
                    try:
                        stats = db.command("collStats", coll_name)
                        count = stats.get('count', 0)
                        self.mongo_tree.insert(db_node, 'end', text=f"📄 {coll_name} ({count} docs)",
                                               values=(db_name, coll_name), tags=('collection',))
                    except:
                        self.mongo_tree.insert(db_node, 'end', text=f"📄 {coll_name}",
                                               values=(db_name, coll_name), tags=('collection',))

            self.status_bar.config(text=f"Loaded {len(db_list)} databases")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to refresh database list:\n{str(e)}")

    def on_mongo_tree_select(self, event):
        selected = self.mongo_tree.selection()
        if not selected:
            return

        item = selected[0]
        tags = self.mongo_tree.item(item, 'tags')

        if 'collection' in tags and self.mongo_query_tabs:
            values = self.mongo_tree.item(item, 'values')
            if values and len(values) >= 2:
                db_name = values[0]
                coll_name = values[1]

                # Fill in current tab
                current_tab = self.mongo_query_tabs[self.mongo_query_notebook.index('current')]
                current_tab['db_entry'].delete(0, 'end')
                current_tab['db_entry'].insert(0, db_name)
                current_tab['coll_entry'].delete(0, 'end')
                current_tab['coll_entry'].insert(0, coll_name)

    def on_mongo_tree_double_click(self, event):
        """Handle double-click on collection to show preview"""
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

                # Auto-execute find query
                if self.mongo_query_tabs:
                    current_tab = self.mongo_query_tabs[self.mongo_query_notebook.index('current')]
                    current_tab['db_entry'].delete(0, 'end')
                    current_tab['db_entry'].insert(0, db_name)
                    current_tab['coll_entry'].delete(0, 'end')
                    current_tab['coll_entry'].insert(0, coll_name)
                    current_tab['query_text'].delete('1.0', 'end')
                    current_tab['query_text'].insert('1.0', '{}')

                    self.execute_mongo_query_tab(
                        current_tab['db_entry'],
                        current_tab['coll_entry'],
                        current_tab['query_text'],
                        current_tab['query_type_var'],
                        current_tab['limit_entry'],
                        current_tab['skip_entry'],
                        current_tab['result_text'],
                        current_tab['time_label']
                    )

    def show_mongo_schema(self, db_entry, coll_entry, result_text):
        """Show collection schema"""
        if not self.mongo_client:
            messagebox.showerror("Error", "Please connect to MongoDB first!")
            return

        database = db_entry.get().strip()
        collection = coll_entry.get().strip()

        if not database or not collection:
            messagebox.showerror("Error", "Please select a database and collection!")
            return

        try:
            db = self.mongo_client[database]
            coll = db[collection]

            sample_docs = list(coll.find().limit(100))

            if not sample_docs:
                messagebox.showinfo("Schema Info", "Collection is empty")
                return

            schema = {}
            for doc in sample_docs:
                for key, value in doc.items():
                    value_type = type(value).__name__
                    if key not in schema:
                        schema[key] = set()
                    schema[key].add(value_type)

            schema_report = {
                "database": database,
                "collection": collection,
                "document_count": coll.count_documents({}),
                "sample_size": len(sample_docs),
                "fields": {key: list(types) for key, types in schema.items()}
            }

            sample_doc = sample_docs[0].copy()
            if '_id' in sample_doc:
                sample_doc['_id'] = str(sample_doc['_id'])
            schema_report["sample_document"] = sample_doc

            result_json = json.dumps(schema_report, indent=2, ensure_ascii=False)
            result_text.delete('1.0', 'end')
            result_text.insert('1.0', result_json)
            result_text.highlight()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to analyze schema:\n{str(e)}")

    def show_mongo_indexes(self, db_entry, coll_entry, result_text):
        """Show collection indexes"""
        if not self.mongo_client:
            messagebox.showerror("Error", "Please connect to MongoDB first!")
            return

        database = db_entry.get().strip()
        collection = coll_entry.get().strip()

        if not database or not collection:
            messagebox.showerror("Error", "Please select a database and collection!")
            return

        try:
            db = self.mongo_client[database]
            coll = db[collection]

            indexes = list(coll.list_indexes())

            result_json = json.dumps(indexes, indent=2, ensure_ascii=False, default=str)
            result_text.delete('1.0', 'end')
            result_text.insert('1.0', result_json)
            result_text.highlight()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to get indexes:\n{str(e)}")

    def show_mongo_stats(self, db_entry, coll_entry, result_text):
        """Show collection statistics"""
        if not self.mongo_client:
            messagebox.showerror("Error", "Please connect to MongoDB first!")
            return

        database = db_entry.get().strip()
        collection = coll_entry.get().strip()

        if not database or not collection:
            messagebox.showerror("Error", "Please select a database and collection!")
            return

        try:
            db = self.mongo_client[database]
            stats = db.command("collStats", collection)

            result_json = json.dumps(stats, indent=2, ensure_ascii=False, default=str)
            result_text.delete('1.0', 'end')
            result_text.insert('1.0', result_json)
            result_text.highlight()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to get stats:\n{str(e)}")

    # Redis Methods
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
            self.status_bar.config(text="Connected to Redis")
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
            for item in self.redis_tree.get_children():
                self.redis_tree.delete(item)

            pattern = self.redis_pattern.get() or "*"
            keys = self.redis_client.keys(pattern)

            if len(keys) > 1000:
                keys = keys[:1000]
                messagebox.showwarning("Warning", "Showing first 1000 keys only")

            key_groups = defaultdict(list)
            for key in sorted(keys):
                if ':' in key:
                    prefix = key.split(':', 1)[0]
                    key_groups[prefix].append(key)
                else:
                    key_groups['_root'].append(key)

            for group, group_keys in sorted(key_groups.items()):
                if group == '_root':
                    for key in group_keys:
                        key_type = self.redis_client.type(key)
                        self.redis_tree.insert('', 'end', text=f"🔑 {key}",
                                               values=(key_type,), tags=('key', key))
                else:
                    group_node = self.redis_tree.insert('', 'end', text=f"📂 {group}",
                                                         tags=('group',))
                    for key in group_keys:
                        key_type = self.redis_client.type(key)
                        display_name = key.split(':', 1)[1] if ':' in key else key
                        self.redis_tree.insert(group_node, 'end', text=f"🔑 {display_name}",
                                               values=(key_type,), tags=('key', key))

            self.status_bar.config(text=f"Loaded {len(keys)} keys")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to refresh key list:\n{str(e)}")

    def on_redis_tree_select(self, event):
        selected = self.redis_tree.selection()
        if not selected:
            return

        item = selected[0]
        tags = self.redis_tree.item(item, 'tags')

        if 'key' in tags:
            key_name = None
            for tag in tags:
                if tag != 'key':
                    key_name = tag
                    break

            if not key_name:
                text = self.redis_tree.item(item, 'text')
                key_name = text.replace('🔑 ', '')

            self.redis_key.delete(0, 'end')
            self.redis_key.insert(0, key_name)

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

                if result is not None:
                    result_json = json.dumps(result, indent=2, ensure_ascii=False, default=str)
                    self.redis_result.delete('1.0', 'end')
                    self.redis_result.insert('1.0', result_json)
                    self.redis_result.highlight()

            except Exception as e:
                self.redis_result.delete('1.0', 'end')
                self.redis_result.insert('1.0', f"Error: {str(e)}")

    def on_redis_tree_double_click(self, event):
        """Handle double-click on key to edit"""
        self.edit_redis_value()

    def on_redis_command_change(self, event):
        cmd = self.redis_command.get()
        if cmd == "CUSTOM":
            self.redis_custom.config(state='normal')
        else:
            self.redis_custom.config(state='disabled')

    def execute_redis_command(self):
        if not self.redis_client:
            messagebox.showerror("Error", "Please connect to Redis first!")
            return

        try:
            cmd = self.redis_command.get()
            key = self.redis_key.get()
            value = self.redis_value.get('1.0', 'end-1c').strip()

            start_time = time.time()
            result = None

            if cmd == "GET":
                result = self.redis_client.get(key)
            elif cmd == "SET":
                result = self.redis_client.set(key, value)
            elif cmd == "DEL":
                result = self.redis_client.delete(key)
                self.refresh_redis_tree()
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
            elif cmd == "DBSIZE":
                result = self.redis_client.dbsize()
            elif cmd == "CUSTOM":
                custom_cmd = self.redis_custom.get('1.0', 'end-1c').strip()
                cmd_list = json.loads(custom_cmd)
                result = self.redis_client.execute_command(*cmd_list)

            execution_time = time.time() - start_time

            result_json = json.dumps(result, indent=2, ensure_ascii=False, default=str)
            self.redis_result.delete('1.0', 'end')
            self.redis_result.insert('1.0', result_json)
            self.redis_result.highlight()

            self.redis_time_label.config(text=f"Time: {execution_time:.3f}s")

            # Add to history
            cmd_str = f"{cmd} {key} {value}" if value else f"{cmd} {key}"
            self.config_manager.add_to_history('redis', cmd_str, execution_time=execution_time)

            self.status_bar.config(text=f"Command executed in {execution_time:.3f}s")

        except json.JSONDecodeError as e:
            messagebox.showerror("JSON Error", f"Invalid JSON:\n{str(e)}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to execute command:\n{str(e)}")

    def edit_redis_value(self):
        """Open editor for Redis value"""
        if not self.redis_client:
            messagebox.showerror("Error", "Please connect to Redis first!")
            return

        key = self.redis_key.get().strip()
        if not key:
            messagebox.showwarning("Warning", "Please select or enter a key")
            return

        try:
            key_type = self.redis_client.type(key)
            value = None

            if key_type == 'string':
                value = self.redis_client.get(key) or ''
            elif key_type == 'hash':
                value = self.redis_client.hgetall(key)
            elif key_type == 'list':
                value = self.redis_client.lrange(key, 0, -1)
            elif key_type == 'set':
                value = list(self.redis_client.smembers(key))
            elif key_type == 'none':
                value = ''
                key_type = 'string'

            dialog = RedisValueEditor(self.root, key, value, key_type)
            self.root.wait_window(dialog)

            if dialog.result:
                self.redis_client.set(dialog.result['key'], dialog.result['value'])

                if dialog.result['ttl'] > 0:
                    self.redis_client.expire(dialog.result['key'], dialog.result['ttl'])

                messagebox.showinfo("Success", "Value saved successfully")
                self.refresh_redis_tree()

        except Exception as e:
            messagebox.showerror("Error", f"Failed to edit value:\n{str(e)}")

    def delete_redis_key(self):
        """Delete selected Redis key"""
        if not self.redis_client:
            messagebox.showerror("Error", "Please connect to Redis first!")
            return

        key = self.redis_key.get().strip()
        if not key:
            messagebox.showwarning("Warning", "Please select or enter a key")
            return

        if messagebox.askyesno("Confirm", f"Delete key '{key}'?"):
            try:
                self.redis_client.delete(key)
                messagebox.showinfo("Success", "Key deleted successfully")
                self.refresh_redis_tree()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to delete key:\n{str(e)}")

    # Profile Management
    def update_mongo_profiles(self):
        """Update MongoDB profile dropdown"""
        profiles = self.config_manager.get_mongo_profiles()
        profile_names = [p['name'] for p in profiles]
        self.mongo_profile_combo['values'] = profile_names

        # Set last used profile
        last_profile = self.config_manager.get_last_connection('mongo')
        if last_profile and last_profile in profile_names:
            self.mongo_profile_var.set(last_profile)
            self.on_mongo_profile_select(None)

    def update_redis_profiles(self):
        """Update Redis profile dropdown"""
        profiles = self.config_manager.get_redis_profiles()
        profile_names = [p['name'] for p in profiles]
        self.redis_profile_combo['values'] = profile_names

        # Set last used profile
        last_profile = self.config_manager.get_last_connection('redis')
        if last_profile and last_profile in profile_names:
            self.redis_profile_var.set(last_profile)
            self.on_redis_profile_select(None)

    def on_mongo_profile_select(self, event):
        """Load MongoDB profile"""
        profile_name = self.mongo_profile_var.get()
        if not profile_name:
            return

        profiles = self.config_manager.get_mongo_profiles()
        profile = next((p for p in profiles if p['name'] == profile_name), None)

        if profile:
            self.mongo_host.delete(0, 'end')
            self.mongo_host.insert(0, profile['host'])

            self.mongo_port.delete(0, 'end')
            self.mongo_port.insert(0, str(profile['port']))

            self.mongo_username.delete(0, 'end')
            self.mongo_username.insert(0, profile.get('username', ''))

            self.mongo_password.delete(0, 'end')
            self.mongo_password.insert(0, profile.get('password', ''))

    def on_redis_profile_select(self, event):
        """Load Redis profile"""
        profile_name = self.redis_profile_var.get()
        if not profile_name:
            return

        profiles = self.config_manager.get_redis_profiles()
        profile = next((p for p in profiles if p['name'] == profile_name), None)

        if profile:
            self.redis_host.delete(0, 'end')
            self.redis_host.insert(0, profile['host'])

            self.redis_port.delete(0, 'end')
            self.redis_port.insert(0, str(profile['port']))

            self.redis_password.delete(0, 'end')
            self.redis_password.insert(0, profile.get('password', ''))

            self.redis_db.delete(0, 'end')
            self.redis_db.insert(0, str(profile.get('db', 0)))

    def save_mongo_profile(self):
        """Save current MongoDB connection as profile"""
        name = simpledialog.askstring("Save Profile", "Enter profile name:")
        if not name:
            return

        try:
            self.config_manager.add_mongo_profile(
                name=name,
                host=self.mongo_host.get(),
                port=int(self.mongo_port.get()),
                username=self.mongo_username.get(),
                password=self.mongo_password.get()
            )
            messagebox.showinfo("Success", "Profile saved successfully")
            self.update_mongo_profiles()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save profile:\n{str(e)}")

    def save_redis_profile(self):
        """Save current Redis connection as profile"""
        name = simpledialog.askstring("Save Profile", "Enter profile name:")
        if not name:
            return

        try:
            self.config_manager.add_redis_profile(
                name=name,
                host=self.redis_host.get(),
                port=int(self.redis_port.get()),
                password=self.redis_password.get(),
                db=int(self.redis_db.get())
            )
            messagebox.showinfo("Success", "Profile saved successfully")
            self.update_redis_profiles()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save profile:\n{str(e)}")

    def manage_mongo_profiles(self):
        """Open MongoDB profile manager"""
        ProfileManager(self.root, self.config_manager, 'mongo')
        self.update_mongo_profiles()

    def manage_redis_profiles(self):
        """Open Redis profile manager"""
        ProfileManager(self.root, self.config_manager, 'redis')
        self.update_redis_profiles()

    # Favorites
    def add_mongo_favorite(self, db_entry, coll_entry, query_text):
        """Add MongoDB query to favorites"""
        name = simpledialog.askstring("Add Favorite", "Enter favorite name:")
        if not name:
            return

        try:
            self.config_manager.add_favorite(
                'mongo',
                name=name,
                query=query_text.get('1.0', 'end-1c'),
                database=db_entry.get(),
                collection=coll_entry.get()
            )
            messagebox.showinfo("Success", "Added to favorites")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add favorite:\n{str(e)}")

    def add_redis_favorite(self):
        """Add Redis command to favorites"""
        name = simpledialog.askstring("Add Favorite", "Enter favorite name:")
        if not name:
            return

        try:
            cmd = self.redis_command.get()
            key = self.redis_key.get()
            value = self.redis_value.get('1.0', 'end-1c')

            query = f"{cmd} {key} {value}" if value else f"{cmd} {key}"

            self.config_manager.add_favorite(
                'redis',
                name=name,
                query=query
            )
            messagebox.showinfo("Success", "Added to favorites")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to add favorite:\n{str(e)}")

    def show_favorites(self):
        """Show favorites dialog"""
        current_tab = self.notebook.tab(self.notebook.select(), 'text')
        db_type = 'mongo' if 'MongoDB' in current_tab else 'redis'

        dialog = FavoritesDialog(self.root, self.config_manager, db_type, self)
        self.root.wait_window(dialog)

    def show_history(self):
        """Show history dialog"""
        current_tab = self.notebook.tab(self.notebook.select(), 'text')
        db_type = 'mongo' if 'MongoDB' in current_tab else 'redis'

        dialog = HistoryDialog(self.root, self.config_manager, db_type, self)
        self.root.wait_window(dialog)

    # Export/Import
    def export_results(self):
        """Export current results"""
        current_tab = self.notebook.tab(self.notebook.select(), 'text')

        if 'MongoDB' in current_tab and self.mongo_query_tabs:
            current_mongo_tab = self.mongo_query_tabs[self.mongo_query_notebook.index('current')]
            result_text = current_mongo_tab['result_text'].get('1.0', 'end-1c')
        elif 'Redis' in current_tab:
            result_text = self.redis_result.get('1.0', 'end-1c')
        else:
            messagebox.showwarning("Warning", "No results to export")
            return

        if not result_text.strip():
            messagebox.showwarning("Warning", "No results to export")
            return

        # Ask for format
        format_choice = messagebox.askquestion("Export Format",
                                               "Export as CSV?\n\nYes = CSV\nNo = JSON",
                                               icon='question')

        if format_choice == 'yes':
            filename = filedialog.asksaveasfilename(
                defaultextension='.csv',
                filetypes=[('CSV files', '*.csv'), ('All files', '*.*')]
            )
            if filename:
                try:
                    data = json.loads(result_text)
                    if isinstance(data, list) and data:
                        import pandas as pd
                        df = pd.DataFrame(data)
                        df.to_csv(filename, index=False)
                        messagebox.showinfo("Success", f"Exported to {filename}")
                    else:
                        messagebox.showerror("Error", "Data format not suitable for CSV export")
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to export:\n{str(e)}")
        else:
            filename = filedialog.asksaveasfilename(
                defaultextension='.json',
                filetypes=[('JSON files', '*.json'), ('All files', '*.*')]
            )
            if filename:
                try:
                    with open(filename, 'w', encoding='utf-8') as f:
                        f.write(result_text)
                    messagebox.showinfo("Success", f"Exported to {filename}")
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to export:\n{str(e)}")

    def import_data(self):
        """Import data from file"""
        filename = filedialog.askopenfilename(
            filetypes=[('JSON files', '*.json'), ('CSV files', '*.csv'), ('All files', '*.*')]
        )

        if not filename:
            return

        current_tab = self.notebook.tab(self.notebook.select(), 'text')

        try:
            if filename.endswith('.json'):
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            elif filename.endswith('.csv'):
                import pandas as pd
                df = pd.read_csv(filename)
                data = df.to_dict('records')
            else:
                messagebox.showerror("Error", "Unsupported file format")
                return

            if 'MongoDB' in current_tab:
                if not isinstance(data, list):
                    data = [data]

                if self.mongo_client and self.mongo_query_tabs:
                    current_mongo_tab = self.mongo_query_tabs[self.mongo_query_notebook.index('current')]
                    db = current_mongo_tab['db_entry'].get().strip()
                    coll = current_mongo_tab['coll_entry'].get().strip()

                    if db and coll:
                        if messagebox.askyesno("Confirm", f"Import {len(data)} documents to {db}.{coll}?"):
                            collection = self.mongo_client[db][coll]
                            result = collection.insert_many(data)
                            messagebox.showinfo("Success", f"Imported {len(result.inserted_ids)} documents")
                    else:
                        messagebox.showwarning("Warning", "Please specify database and collection")
                else:
                    messagebox.showwarning("Warning", "Please connect to MongoDB first")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to import:\n{str(e)}")

    # Theme
    def toggle_theme(self):
        """Toggle between light and dark theme"""
        current_theme = self.config_manager.get_setting('theme', 'light')
        new_theme = 'dark' if current_theme == 'light' else 'light'
        self.config_manager.update_setting('theme', new_theme)
        self.apply_theme()

    def apply_theme(self):
        """Apply current theme"""
        theme = self.config_manager.get_setting('theme', 'light')

        if theme == 'dark':
            bg_color = '#2b2b2b'
            fg_color = '#ffffff'
            entry_bg = '#3c3f41'
            entry_fg = '#a9b7c6'
        else:
            bg_color = '#ffffff'
            fg_color = '#000000'
            entry_bg = '#ffffff'
            entry_fg = '#000000'

        # Apply to root
        self.root.configure(bg=bg_color)

        # Note: Full theme support would require custom ttk theme
        # This is a simplified version

    def clear_results(self):
        """Clear current results"""
        current_tab = self.notebook.tab(self.notebook.select(), 'text')

        if 'MongoDB' in current_tab and self.mongo_query_tabs:
            current_mongo_tab = self.mongo_query_tabs[self.mongo_query_notebook.index('current')]
            current_mongo_tab['result_text'].delete('1.0', 'end')
        elif 'Redis' in current_tab:
            self.redis_result.delete('1.0', 'end')

    def show_settings(self):
        """Show settings dialog"""
        SettingsDialog(self.root, self.config_manager)

    def show_about(self):
        """Show about dialog"""
        messagebox.showinfo("About",
                           "MongoDB & Redis Query Tool - Advanced\n\n"
                           "Version 2.0\n\n"
                           "A comprehensive GUI tool for MongoDB and Redis\n"
                           "with advanced features including:\n"
                           "- Connection profiles\n"
                           "- Query history & favorites\n"
                           "- Data editing\n"
                           "- Export/Import\n"
                           "- Performance monitoring\n"
                           "- And much more!")


class FavoritesDialog(tk.Toplevel):
    """Dialog for managing favorites"""

    def __init__(self, parent, config_manager, db_type, main_app):
        super().__init__(parent)
        self.config_manager = config_manager
        self.db_type = db_type
        self.main_app = main_app
        self.title(f"{db_type.upper()} Favorites")
        self.geometry("800x500")

        # List frame
        list_frame = ttk.LabelFrame(self, text="Favorites", padding=10)
        list_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Treeview
        columns = ('name', 'query', 'database', 'collection')
        self.tree = ttk.Treeview(list_frame, columns=columns, show='headings')

        self.tree.heading('name', text='Name')
        self.tree.heading('query', text='Query')
        self.tree.heading('database', text='Database')
        self.tree.heading('collection', text='Collection')

        self.tree.column('name', width=150)
        self.tree.column('query', width=300)
        self.tree.column('database', width=100)
        self.tree.column('collection', width=100)

        self.tree.pack(fill='both', expand=True, side='left')

        scrollbar = ttk.Scrollbar(list_frame, orient='vertical', command=self.tree.yview)
        scrollbar.pack(side='right', fill='y')
        self.tree.configure(yscrollcommand=scrollbar.set)

        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)

        ttk.Button(btn_frame, text="Load", command=self.load_favorite).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Delete", command=self.delete_favorite).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Close", command=self.destroy).pack(side='right', padx=5)

        self.refresh_list()

        self.transient(parent)

    def refresh_list(self):
        """Refresh favorites list"""
        self.tree.delete(*self.tree.get_children())

        favorites = self.config_manager.get_favorites(self.db_type)
        for fav in favorites:
            self.tree.insert('', 'end', values=(
                fav['name'],
                fav['query'][:50] + '...' if len(fav['query']) > 50 else fav['query'],
                fav.get('database', ''),
                fav.get('collection', '')
            ))

    def load_favorite(self):
        """Load selected favorite"""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Please select a favorite")
            return

        item = self.tree.item(selected[0])
        fav_name = item['values'][0]

        favorites = self.config_manager.get_favorites(self.db_type)
        fav = next((f for f in favorites if f['name'] == fav_name), None)

        if fav:
            if self.db_type == 'mongo' and self.main_app.mongo_query_tabs:
                current_tab = self.main_app.mongo_query_tabs[
                    self.main_app.mongo_query_notebook.index('current')
                ]

                if fav.get('database'):
                    current_tab['db_entry'].delete(0, 'end')
                    current_tab['db_entry'].insert(0, fav['database'])

                if fav.get('collection'):
                    current_tab['coll_entry'].delete(0, 'end')
                    current_tab['coll_entry'].insert(0, fav['collection'])

                current_tab['query_text'].delete('1.0', 'end')
                current_tab['query_text'].insert('1.0', fav['query'])
                current_tab['query_text'].highlight()

            elif self.db_type == 'redis':
                # Parse command
                parts = fav['query'].split(maxsplit=2)
                if len(parts) >= 1:
                    self.main_app.redis_command.set(parts[0])
                if len(parts) >= 2:
                    self.main_app.redis_key.delete(0, 'end')
                    self.main_app.redis_key.insert(0, parts[1])
                if len(parts) >= 3:
                    self.main_app.redis_value.delete('1.0', 'end')
                    self.main_app.redis_value.insert('1.0', parts[2])

            messagebox.showinfo("Success", "Favorite loaded")
            self.destroy()

    def delete_favorite(self):
        """Delete selected favorite"""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Please select a favorite")
            return

        item = self.tree.item(selected[0])
        fav_name = item['values'][0]

        if messagebox.askyesno("Confirm", f"Delete favorite '{fav_name}'?"):
            self.config_manager.delete_favorite(self.db_type, fav_name)
            self.refresh_list()


class HistoryDialog(tk.Toplevel):
    """Dialog for viewing query history"""

    def __init__(self, parent, config_manager, db_type, main_app):
        super().__init__(parent)
        self.config_manager = config_manager
        self.db_type = db_type
        self.main_app = main_app
        self.title(f"{db_type.upper()} Query History")
        self.geometry("900x500")

        # List frame
        list_frame = ttk.LabelFrame(self, text="History", padding=10)
        list_frame.pack(fill='both', expand=True, padx=10, pady=10)

        # Treeview
        columns = ('time', 'query', 'database', 'collection', 'exec_time')
        self.tree = ttk.Treeview(list_frame, columns=columns, show='headings')

        self.tree.heading('time', text='Time')
        self.tree.heading('query', text='Query')
        self.tree.heading('database', text='Database')
        self.tree.heading('collection', text='Collection')
        self.tree.heading('exec_time', text='Exec Time (s)')

        self.tree.column('time', width=150)
        self.tree.column('query', width=350)
        self.tree.column('database', width=100)
        self.tree.column('collection', width=100)
        self.tree.column('exec_time', width=100)

        self.tree.pack(fill='both', expand=True, side='left')

        scrollbar = ttk.Scrollbar(list_frame, orient='vertical', command=self.tree.yview)
        scrollbar.pack(side='right', fill='y')
        self.tree.configure(yscrollcommand=scrollbar.set)

        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)

        ttk.Button(btn_frame, text="Load", command=self.load_history).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Clear History", command=self.clear_history).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Close", command=self.destroy).pack(side='right', padx=5)

        self.refresh_list()

        self.transient(parent)

    def refresh_list(self):
        """Refresh history list"""
        self.tree.delete(*self.tree.get_children())

        history = self.config_manager.get_history(self.db_type)
        for item in history:
            timestamp = item.get('timestamp', '')
            if timestamp:
                try:
                    dt = datetime.fromisoformat(timestamp)
                    timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass

            self.tree.insert('', 'end', values=(
                timestamp,
                item['query'][:60] + '...' if len(item['query']) > 60 else item['query'],
                item.get('database', ''),
                item.get('collection', ''),
                f"{item.get('execution_time', 0):.3f}"
            ))

    def load_history(self):
        """Load selected history item"""
        selected = self.tree.selection()
        if not selected:
            messagebox.showwarning("Warning", "Please select a history item")
            return

        # Get index
        index = self.tree.index(selected[0])
        history = self.config_manager.get_history(self.db_type)

        if index < len(history):
            item = history[index]

            if self.db_type == 'mongo' and self.main_app.mongo_query_tabs:
                current_tab = self.main_app.mongo_query_tabs[
                    self.main_app.mongo_query_notebook.index('current')
                ]

                if item.get('database'):
                    current_tab['db_entry'].delete(0, 'end')
                    current_tab['db_entry'].insert(0, item['database'])

                if item.get('collection'):
                    current_tab['coll_entry'].delete(0, 'end')
                    current_tab['coll_entry'].insert(0, item['collection'])

                current_tab['query_text'].delete('1.0', 'end')
                current_tab['query_text'].insert('1.0', item['query'])
                current_tab['query_text'].highlight()

            elif self.db_type == 'redis':
                # Parse command
                parts = item['query'].split(maxsplit=2)
                if len(parts) >= 1:
                    self.main_app.redis_command.set(parts[0])
                if len(parts) >= 2:
                    self.main_app.redis_key.delete(0, 'end')
                    self.main_app.redis_key.insert(0, parts[1])
                if len(parts) >= 3:
                    self.main_app.redis_value.delete('1.0', 'end')
                    self.main_app.redis_value.insert('1.0', parts[2])

            messagebox.showinfo("Success", "History item loaded")
            self.destroy()

    def clear_history(self):
        """Clear all history"""
        if messagebox.askyesno("Confirm", "Clear all history?"):
            self.config_manager.clear_history(self.db_type)
            self.refresh_list()


class SettingsDialog(tk.Toplevel):
    """Dialog for application settings"""

    def __init__(self, parent, config_manager):
        super().__init__(parent)
        self.config_manager = config_manager
        self.title("Settings")
        self.geometry("500x400")

        # Settings frame
        settings_frame = ttk.LabelFrame(self, text="Settings", padding=20)
        settings_frame.pack(fill='both', expand=True, padx=10, pady=10)

        row = 0

        # Theme
        ttk.Label(settings_frame, text="Theme:").grid(row=row, column=0, sticky='w', pady=10)
        self.theme_var = tk.StringVar(value=config_manager.get_setting('theme', 'light'))
        theme_frame = ttk.Frame(settings_frame)
        theme_frame.grid(row=row, column=1, sticky='w', pady=10)
        ttk.Radiobutton(theme_frame, text="Light", variable=self.theme_var, value='light').pack(side='left', padx=5)
        ttk.Radiobutton(theme_frame, text="Dark", variable=self.theme_var, value='dark').pack(side='left', padx=5)
        row += 1

        # Max history
        ttk.Label(settings_frame, text="Max History Items:").grid(row=row, column=0, sticky='w', pady=10)
        self.max_history_var = tk.StringVar(value=str(config_manager.get_setting('max_history', 50)))
        ttk.Entry(settings_frame, textvariable=self.max_history_var, width=10).grid(row=row, column=1, sticky='w', pady=10)
        row += 1

        # Page size
        ttk.Label(settings_frame, text="Default Page Size:").grid(row=row, column=0, sticky='w', pady=10)
        self.page_size_var = tk.StringVar(value=str(config_manager.get_setting('page_size', 100)))
        ttk.Entry(settings_frame, textvariable=self.page_size_var, width=10).grid(row=row, column=1, sticky='w', pady=10)
        row += 1

        # Auto refresh
        ttk.Label(settings_frame, text="Auto Refresh:").grid(row=row, column=0, sticky='w', pady=10)
        self.auto_refresh_var = tk.BooleanVar(value=config_manager.get_setting('auto_refresh', False))
        ttk.Checkbutton(settings_frame, variable=self.auto_refresh_var).grid(row=row, column=1, sticky='w', pady=10)
        row += 1

        # Refresh interval
        ttk.Label(settings_frame, text="Refresh Interval (s):").grid(row=row, column=0, sticky='w', pady=10)
        self.refresh_interval_var = tk.StringVar(value=str(config_manager.get_setting('refresh_interval', 30)))
        ttk.Entry(settings_frame, textvariable=self.refresh_interval_var, width=10).grid(row=row, column=1, sticky='w', pady=10)
        row += 1

        # Buttons
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill='x', padx=10, pady=10)

        ttk.Button(btn_frame, text="Save", command=self.save).pack(side='left', padx=5)
        ttk.Button(btn_frame, text="Cancel", command=self.destroy).pack(side='right', padx=5)

        self.transient(parent)
        self.grab_set()

    def save(self):
        """Save settings"""
        try:
            self.config_manager.update_setting('theme', self.theme_var.get())
            self.config_manager.update_setting('max_history', int(self.max_history_var.get()))
            self.config_manager.update_setting('page_size', int(self.page_size_var.get()))
            self.config_manager.update_setting('auto_refresh', self.auto_refresh_var.get())
            self.config_manager.update_setting('refresh_interval', int(self.refresh_interval_var.get()))

            messagebox.showinfo("Success", "Settings saved successfully")
            self.destroy()
        except ValueError:
            messagebox.showerror("Error", "Please enter valid numbers")


if __name__ == "__main__":
    root = tk.Tk()
    app = DatabaseQueryTool(root)
    root.mainloop()
