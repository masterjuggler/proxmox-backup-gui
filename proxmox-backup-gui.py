import sys
import os
import json
import subprocess
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional

import yaml
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QLineEdit, QTabWidget, QTableWidget,
    QTableWidgetItem, QFileDialog, QMessageBox, QProgressBar,
    QProgressDialog, QInputDialog, QSystemTrayIcon, QMenu, QComboBox,
    QListWidget, QListWidgetItem, QDialog, QTextEdit, QHeaderView
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QIcon, QClipboard


# log_dir = Path.home() / '.config' / 'proxmox-backup-gui' / 'logs'
# log_dir.mkdir(parents=True, exist_ok=True)
# log_file = log_dir / f'proxmox-backup-gui_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.log'

# logging.basicConfig(
#     filename=log_file,
#     level=logging.INFO,
#     format='%(asctime)s %(levelname)s: %(message)s',
#     datefmt='%Y-%m-%d %H:%M:%S'
# )

# logger = logging.getLogger(__name__)

class BackupSource:
    def __init__(self, path: str, archive_type: str = 'pxar', exclusions: List[str] = None):
        self.path = path
        self.archive_type = archive_type
        self.exclusions = exclusions or []

    def to_dict(self) -> dict:
        return {
            'path': self.path,
            'archive_type': self.archive_type,
            'exclusions': self.exclusions
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'BackupSource':
        return cls(data['path'], data['archive_type'], data.get('exclusions', []))

    def __str__(self) -> str:
        exclusions_str = f" (excludes: {', '.join(self.exclusions)})" if self.exclusions else ""
        return f"{self.path} ({self.archive_type}){exclusions_str}"

class BackupWorker(QThread):
    progress = pyqtSignal(str)
    finished = pyqtSignal(bool, str)
    command_ready = pyqtSignal(list)

    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent

    def get_backup_command(self) -> list:
        config = self.parent.get_current_config()
        cmd = ['proxmox-backup-client', 'backup']
        
        # Add backup sources and their exclusions
        for source in config['backup_sources']:
            dir_name = os.path.basename(source.path.rstrip('/'))
            cmd.append(f"{dir_name}.{source.archive_type}:{source.path}")
            # Add exclusions for this source
            for exclusion in source.exclusions:
                cmd.append(f"--exclude={exclusion}")

        cmd.extend([f"--repository", config['repository']])
        return cmd

    def run(self):
        try:
            config = self.parent.get_current_config()
            env = dict(os.environ)
            env['PBS_PASSWORD'] = config['api_key']
            if config.get('fingerprint'):  # Only set fingerprint if it exists
                env['PBS_FINGERPRINT'] = config['fingerprint']
            
            cmd = self.get_backup_command()
            self.command_ready.emit(cmd)

            self.progress.emit(f"Running command: {' '.join(cmd)}")

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                env=env
            )

            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    self.progress.emit(output.strip())

            returncode = process.poll()
            
            if returncode == 0:
                self.finished.emit(True, "Backup completed successfully")
            else:
                error = process.stderr.read()
                self.finished.emit(False, f"Backup failed: {error}")
                
        except Exception as e:
            self.finished.emit(False, f"Error: {str(e)}")

class BackupProfile:
    def __init__(self, name: str, repository: str = '', api_key: str = '', fingerprint: str = '', backup_sources: List[BackupSource] = None):
        self.name = name
        self.repository = repository
        self.api_key = api_key
        self.fingerprint = fingerprint
        self.backup_sources = backup_sources or []

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'repository': self.repository,
            'api_key': self.api_key,
            'fingerprint': self.fingerprint,
            'backup_sources': [source.to_dict() for source in self.backup_sources]
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'BackupProfile':
        sources = [BackupSource.from_dict(source) for source in data.get('backup_sources', [])]
        return cls(
            data['name'],
            data['repository'],
            data['api_key'],
            data.get('fingerprint', ''),  # Make fingerprint optional for backward compatibility
            sources
        )

class ProxmoxBackupGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.version = "1.0.0"  # Add version tracking
        self.setGeometry(100, 100, 800, 600)
        
        # Create config path
        self.config_file = Path.home() / '.config' / 'proxmox-backup-gui' / 'config.yaml'
        self.config_file.parent.mkdir(parents=True, exist_ok=True)

        # Create and set icon
        icon_path = self.create_default_icon()
        self.icon = QIcon(icon_path)
        self.setWindowIcon(self.icon)

        # Track current mount
        self.current_mount = None
        
        # Initialize profiles
        self.profiles: Dict[str, BackupProfile] = {}
        self.current_profile_name = None

        # Load config
        self.load_config()

        # Setup UI
        self.setup_ui()
        
        # Setup system tray
        self.setup_tray()

        # Refresh archives for current profile
        self.refresh_archives()

    def create_default_icon(self):
        """Create a default icon if none exists"""
        config_dir = Path.home() / '.config' / 'proxmox-backup-gui'
        icon_path = config_dir / 'icon.svg'
        
        if not icon_path.exists():
            svg_content = '''<?xml version="1.0" encoding="UTF-8"?>
<svg width="64" height="64" version="1.1" viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg">
 <circle cx="32" cy="32" r="28" fill="#2196f3" stroke="#1976d2" stroke-width="2"/>
 <path d="m32 16v32m-16-16h32" stroke="#fff" stroke-linecap="round" stroke-width="4"/>
</svg>'''
            
            icon_path.write_text(svg_content)
        
        return str(icon_path)

    def setup_ui(self):
        # Create central widget and main layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # Add profile selector at the top
        profile_layout = QHBoxLayout()
        profile_layout.addWidget(QLabel("Profile:"))
        
        self.profile_combo = QComboBox()
        self.profile_combo.currentTextChanged.connect(self.switch_profile)
        profile_layout.addWidget(self.profile_combo)
        
        new_profile_button = QPushButton("New Profile")
        new_profile_button.clicked.connect(self.create_new_profile)
        profile_layout.addWidget(new_profile_button)
        
        delete_profile_button = QPushButton("Delete Profile")
        delete_profile_button.clicked.connect(self.delete_current_profile)
        profile_layout.addWidget(delete_profile_button)
        
        layout.addLayout(profile_layout)

        # Create tab widget
        tabs = QTabWidget()
        layout.addWidget(tabs)

        # Create tabs
        self.create_backup_tab(tabs)
        self.create_archives_tab(tabs)
        self.create_settings_tab(tabs)
        
        # Update profile selector
        self.update_profile_selector()

    def create_backup_tab(self, tabs):
        backup_tab = QWidget()
        layout = QVBoxLayout(backup_tab)

        # Source list
        sources_label = QLabel("Backup Sources:")
        layout.addWidget(sources_label)
        
        self.sources_list = QListWidget()
        self.update_sources_list()
        layout.addWidget(self.sources_list)

        # Add source controls
        add_layout = QHBoxLayout()
        
        self.source_edit = QLineEdit()
        self.source_edit.setPlaceholderText("Select directory to backup...")
        browse_button = QPushButton("Browse")
        browse_button.clicked.connect(self.browse_source)
        
        self.archive_combo = QComboBox()
        self.archive_combo.addItems(['pxar', 'img'])
        
        exclusions_button = QPushButton("Edit Exclusions")
        exclusions_button.clicked.connect(self.edit_exclusions)
        
        add_button = QPushButton("Add Source")
        add_button.clicked.connect(self.add_source)
        
        remove_button = QPushButton("Remove Selected")
        remove_button.clicked.connect(self.remove_source)

        add_layout.addWidget(self.source_edit)
        add_layout.addWidget(browse_button)
        add_layout.addWidget(self.archive_combo)
        add_layout.addWidget(exclusions_button)
        add_layout.addWidget(add_button)
        add_layout.addWidget(remove_button)
        
        layout.addLayout(add_layout)

        # Command preview section
        command_layout = QVBoxLayout()
        command_label = QLabel("Backup Command:")
        command_layout.addWidget(command_label)
        
        self.command_display = QLineEdit()
        self.command_display.setReadOnly(True)
        self.command_display.setPlaceholderText("Command will appear here when backup sources are added...")
        command_layout.addWidget(self.command_display)
        
        copy_button = QPushButton("Copy Command")
        copy_button.clicked.connect(self.copy_command)
        command_layout.addWidget(copy_button)
        
        layout.addLayout(command_layout)

        # Progress display
        self.progress_label = QLabel("Ready")
        layout.addWidget(self.progress_label)
        self.progress_bar = QProgressBar()
        layout.addWidget(self.progress_bar)

        # Backup button
        backup_button = QPushButton("Start Backup")
        backup_button.clicked.connect(self.start_backup)
        layout.addWidget(backup_button)

        layout.addStretch()
        tabs.addTab(backup_tab, "Backup")

    def edit_exclusions(self):
        config = self.get_current_config()
        current_row = self.sources_list.currentRow()
        if current_row < 0:
            QMessageBox.warning(self, "Error", "Please select a source first")
            return

        source = config['backup_sources'][current_row]
        
        # Create dialog for editing exclusions
        dialog = QDialog(self)
        dialog.setWindowTitle("Edit Exclusions")
        layout = QVBoxLayout(dialog)
        
        # Explanation label
        layout.addWidget(QLabel("Enter one exclusion path per line:"))
        
        # Text edit for exclusions
        text_edit = QTextEdit()
        text_edit.setPlainText("\n".join(source.exclusions))
        layout.addWidget(text_edit)
        
        # Buttons
        button_box = QHBoxLayout()
        save_button = QPushButton("Save")
        cancel_button = QPushButton("Cancel")
        
        button_box.addWidget(save_button)
        button_box.addWidget(cancel_button)
        layout.addLayout(button_box)
        
        save_button.clicked.connect(dialog.accept)
        cancel_button.clicked.connect(dialog.reject)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            # Update exclusions
            source.exclusions = [line.strip() for line in text_edit.toPlainText().split('\n') if line.strip()]
            self.update_sources_list()
            self.save_settings()

    def update_sources_list(self):
        """Update the sources list for the current profile"""
        self.sources_list.clear()
        if self.current_profile_name:
            profile = self.profiles[self.current_profile_name]
            for source in profile.backup_sources:
                self.sources_list.addItem(str(source))

    def browse_source(self):
        directory = QFileDialog.getExistingDirectory(self, "Select Source Directory")
        if directory:
            self.source_edit.setText(directory)

    def add_source(self):
        """Add a backup source to the current profile"""
        if not self.current_profile_name:
            return
            
        path = self.source_edit.text()
        if not path:
            QMessageBox.warning(self, "Error", "Please specify a source path")
            return

        archive_type = self.archive_combo.currentText()
        source = BackupSource(path, archive_type)
        
        profile = self.profiles[self.current_profile_name]
        profile.backup_sources.append(source)
        
        self.update_sources_list()
        self.update_command_display()
        self.save_settings()
        self.source_edit.clear()

    def remove_source(self):
        """Remove a backup source from the current profile"""
        if not self.current_profile_name:
            return
            
        current_row = self.sources_list.currentRow()
        if current_row >= 0:
            profile = self.profiles[self.current_profile_name]
            profile.backup_sources.pop(current_row)
            self.update_sources_list()
            self.update_command_display()
            self.save_settings()

    def start_backup(self):
        if not self.validate_config():
            return

        self.worker = BackupWorker(self)
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.backup_finished)
        self.worker.command_ready.connect(lambda cmd: self.command_display.setText(' '.join(cmd)))
        self.worker.start()

        self.progress_bar.setRange(0, 0)  # Show indeterminate progress
        self.progress_label.setText("Backup in progress...")

    def update_command_display(self):
        """Update the command display based on current settings"""
        if hasattr(self, 'command_display'):  # Check if UI is initialized
            if not self.validate_config(show_message=False):
                self.command_display.setText("")
                return
                
            worker = BackupWorker(self)
            cmd = worker.get_backup_command()
            self.command_display.setText(' '.join(cmd))

    def copy_command(self):
        command = self.command_display.text()
        if command:
            clipboard = QApplication.clipboard()
            clipboard.setText(command)
            QMessageBox.information(self, "Success", "Command copied to clipboard!")
        else:
            QMessageBox.warning(self, "Error", "No command to copy. Please add backup sources first.")

    def validate_config(self, show_message=True) -> bool:
        """Validate the current profile configuration"""
        if not self.current_profile_name:
            if show_message:
                QMessageBox.warning(self, "Error", "No profile selected")
            return False
            
        profile = self.profiles[self.current_profile_name]
        
        if not profile.backup_sources:
            if show_message:
                QMessageBox.warning(self, "Error", "Please add at least one backup source")
            return False
        if not profile.repository:
            if show_message:
                QMessageBox.warning(self, "Error", "Please configure repository in settings")
            return False
        if not profile.api_key:
            if show_message:
                QMessageBox.warning(self, "Error", "Please configure API key in settings")
            return False
        return True

    def get_current_config(self) -> dict:
        """Get the configuration for the current profile"""
        if not self.current_profile_name:
            return {}
            
        profile = self.profiles[self.current_profile_name]
        return {
            'repository': profile.repository,
            'api_key': profile.api_key,
            'fingerprint': profile.fingerprint,
            'backup_sources': profile.backup_sources
        }

    def create_settings_tab(self, tabs):
        config = self.get_current_config()
        settings_tab = QWidget()
        layout = QVBoxLayout(settings_tab)

        # Repository settings
        repo_layout = QHBoxLayout()
        repo_label = QLabel("Repository:")
        self.repo_edit = QLineEdit(config.get('repository', ''))
        repo_layout.addWidget(repo_label)
        repo_layout.addWidget(self.repo_edit)
        layout.addLayout(repo_layout)

        # API Key settings
        api_layout = QHBoxLayout()
        api_label = QLabel("API Key:")
        self.api_edit = QLineEdit(config.get('api_key', ''))
        self.api_edit.setEchoMode(QLineEdit.EchoMode.Password)
        show_api_button = QPushButton("Show/Hide")
        show_api_button.clicked.connect(self.toggle_api_visibility)
        api_layout.addWidget(api_label)
        api_layout.addWidget(self.api_edit)
        api_layout.addWidget(show_api_button)
        layout.addLayout(api_layout)

        # Fingerprint settings
        fingerprint_layout = QHBoxLayout()
        fingerprint_label = QLabel("Server Fingerprint:")
        self.fingerprint_edit = QLineEdit(config.get('fingerprint', ''))
        fingerprint_layout.addWidget(fingerprint_label)
        fingerprint_layout.addWidget(self.fingerprint_edit)
        layout.addLayout(fingerprint_layout)

        # Save button
        save_button = QPushButton("Save Settings")
        save_button.clicked.connect(lambda: self.save_settings(show_message=True))
        layout.addWidget(save_button)

        # Test connection button
        test_button = QPushButton("Test Connection")
        test_button.clicked.connect(self.test_connection)
        layout.addWidget(test_button)

        # Add version display
        version_layout = QHBoxLayout()
        version_layout.addStretch()  # Push version to the right
        version_label = QLabel(f"Version: {self.version}")
        version_label.setStyleSheet("color: gray;")  # Make it subtle
        version_layout.addWidget(version_label)
        layout.addLayout(version_layout)

        layout.addStretch()
        tabs.addTab(settings_tab, "Settings")

    def toggle_api_visibility(self):
        if self.api_edit.echoMode() == QLineEdit.EchoMode.Password:
            self.api_edit.setEchoMode(QLineEdit.EchoMode.Normal)
        else:
            self.api_edit.setEchoMode(QLineEdit.EchoMode.Password)

    def test_connection(self):
        if not self.repo_edit.text() or not self.api_edit.text():
            QMessageBox.warning(self, "Error", "Please enter both repository and API key")
            return

        try:
            env = dict(os.environ)
            env['PBS_PASSWORD'] = self.api_edit.text()
            if self.fingerprint_edit.text():
                env['PBS_FINGERPRINT'] = self.fingerprint_edit.text()
            
            cmd = [
                'proxmox-backup-client',
                'list',
                f"--repository", self.repo_edit.text(),
                '--output-format', 'json'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, env=env)
            
            if result.returncode == 0:
                QMessageBox.information(self, "Success", "Connection successful!")
            else:
                QMessageBox.warning(self, "Error", f"Connection failed: {result.stderr}")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Connection test failed: {str(e)}")

    def setup_tray(self):
        self.tray_icon = QSystemTrayIcon(self)
        self.tray_icon.setIcon(self.icon)  # Set the icon
        self.tray_icon.setToolTip("Proxmox Backup GUI")
        
        # Create tray menu
        tray_menu = QMenu()
        show_action = tray_menu.addAction("Show")
        show_action.triggered.connect(self.show)
        quit_action = tray_menu.addAction("Quit")
        quit_action.triggered.connect(QApplication.quit)
        
        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.show()

    def load_config(self):
        try:
            if self.config_file.exists():
                with open(self.config_file) as f:
                    data = yaml.safe_load(f) or {}
                    
                # Convert old config format to profile format if necessary
                if 'profiles' not in data:
                    # Create default profile from existing config
                    default_profile = {
                        'name': 'Default',
                        'repository': data.get('repository', ''),
                        'api_key': data.get('api_key', ''),
                        'backup_sources': data.get('backup_sources', [])
                    }
                    data = {'profiles': [default_profile]}
                
                # Load profiles
                self.profiles = {}
                for profile_data in data.get('profiles', []):
                    profile = BackupProfile.from_dict(profile_data)
                    self.profiles[profile.name] = profile
                
                # Set current profile
                profile_names = list(self.profiles.keys())
                self.current_profile_name = profile_names[0] if profile_names else None
            
            # If no profiles exist, create a default one
            if not self.profiles:
                default_profile = BackupProfile('Default')
                self.profiles['Default'] = default_profile
                self.current_profile_name = 'Default'
                
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to load config: {str(e)}")
            # Create default profile
            default_profile = BackupProfile('Default')
            self.profiles = {'Default': default_profile}
            self.current_profile_name = 'Default'

    def save_settings(self, show_message=False):
        try:
            # Update current profile from UI
            if self.current_profile_name:
                profile = self.profiles[self.current_profile_name]
                profile.repository = self.repo_edit.text()
                profile.api_key = self.api_edit.text()
                profile.fingerprint = self.fingerprint_edit.text()

            # Prepare config data
            config_data = {
                'profiles': [profile.to_dict() for profile in self.profiles.values()]
            }

            # Ensure config directory exists
            self.config_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Set restrictive permissions for config file
            if self.config_file.exists():
                self.config_file.chmod(0o600)
                
            with open(self.config_file, 'w') as f:
                yaml.dump(config_data, f)
                
            # Set restrictive permissions for new file
            self.config_file.chmod(0o600)
            
            if show_message:
                QMessageBox.information(self, "Success", "Settings saved successfully")
                
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to save settings: {str(e)}")

    def update_profile_selector(self):
        """Update the profile selector combo box"""
        current = self.profile_combo.currentText()
        self.profile_combo.clear()
        self.profile_combo.addItems(sorted(self.profiles.keys()))
        
        # Restore previous selection if it still exists
        if current in self.profiles:
            self.profile_combo.setCurrentText(current)
        else:
            self.profile_combo.setCurrentText(self.current_profile_name)

    def switch_profile(self, profile_name: str):
        """Switch to a different profile"""
        if profile_name and profile_name in self.profiles:
            self.current_profile_name = profile_name
            self.update_ui_from_profile()
            self.update_command_display()

    def update_ui_from_profile(self):
        """Update UI elements with current profile data"""
        if not self.current_profile_name:
            return
            
        profile = self.profiles[self.current_profile_name]
        
        # Update settings
        self.repo_edit.setText(profile.repository)
        self.api_edit.setText(profile.api_key)
        self.fingerprint_edit.setText(profile.fingerprint)
        
        # Update sources list
        self.sources_list.clear()
        for source in profile.backup_sources:
            self.sources_list.addItem(str(source))
            
        # Update command display
        self.update_command_display()

    def create_new_profile(self):
        """Create a new backup profile"""
        name, ok = QInputDialog.getText(self, "New Profile", "Enter profile name:")
        if ok and name:
            if name in self.profiles:
                QMessageBox.warning(self, "Error", "A profile with this name already exists")
                return
                
            self.profiles[name] = BackupProfile(name)
            self.current_profile_name = name
            self.update_profile_selector()
            self.update_ui_from_profile()
            self.save_settings()

    def delete_current_profile(self):
        """Delete the current profile"""
        if not self.current_profile_name:
            return
            
        if len(self.profiles) <= 1:
            QMessageBox.warning(self, "Error", "Cannot delete the last profile")
            return
            
        reply = QMessageBox.question(
            self,
            "Confirm Deletion",
            f"Are you sure you want to delete the profile '{self.current_profile_name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            del self.profiles[self.current_profile_name]
            self.current_profile_name = next(iter(self.profiles))
            self.update_profile_selector()
            self.update_ui_from_profile()
            self.save_settings()

    def closeEvent(self, event):
        if self.current_mount:
            try:
                self.unmount_current()
            except Exception as e:
                QMessageBox.warning(self, "Warning", f"Failed to unmount archive on exit: {str(e)}")
        event.accept()

    def update_progress(self, message: str):
        self.progress_label.setText(message)

    def backup_finished(self, success: bool, message: str):
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(100 if success else 0)
        self.progress_label.setText(message)
        
        if success:
            # Update the last backup time in the current profile
            if self.current_profile_name:
                self.profiles[self.current_profile_name].last_backup = datetime.now().isoformat()
                self.save_settings(show_message=False)
            self.tray_icon.showMessage("Backup Complete", "Backup finished successfully")
        else:
            self.tray_icon.showMessage("Backup Failed", message, QSystemTrayIcon.MessageIcon.Critical)

    def create_archives_tab(self, tabs):
        archives_tab = QWidget()
        layout = QVBoxLayout(archives_tab)

        # Mount status
        self.mount_status_label = QLabel("No archive currently mounted")
        layout.addWidget(self.mount_status_label)

        # Archives table
        self.archives_table = QTableWidget()
        self.archives_table.setColumnCount(5)
        self.archives_table.setHorizontalHeaderLabels(["Archive", "Size", "Date", "Owner", "Verify State"])
        
        # Set column resize modes
        header = self.archives_table.horizontalHeader()
        for i in range(5):
            header.setSectionResizeMode(i, QHeaderView.ResizeMode.ResizeToContents)
        
        layout.addWidget(self.archives_table)

        # Buttons layout
        button_layout = QHBoxLayout()
        
        refresh_button = QPushButton("Refresh")
        refresh_button.clicked.connect(self.refresh_archives)
        button_layout.addWidget(refresh_button)

        restore_button = QPushButton("Restore")
        restore_button.clicked.connect(self.restore_archive)
        button_layout.addWidget(restore_button)

        mount_button = QPushButton("Mount")
        mount_button.clicked.connect(self.mount_archive)
        button_layout.addWidget(mount_button)

        unmount_button = QPushButton("Unmount")
        unmount_button.clicked.connect(self.unmount_current)
        button_layout.addWidget(unmount_button)

        # Add Delete button
        delete_button = QPushButton("Delete")
        delete_button.clicked.connect(self.delete_archive)
        button_layout.addWidget(delete_button)

        layout.addLayout(button_layout)
        tabs.addTab(archives_tab, "Archives")

    def refresh_archives(self):
        try:
            config = self.get_current_config()
            env = dict(os.environ)
            env['PBS_PASSWORD'] = config['api_key']
            
            cmd = [
                'proxmox-backup-client',
                'snapshot',
                'list',
                f"--repository", config['repository'],
                '--output-format', 'json'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, env=env)
            
            if result.returncode == 0:
                archives = json.loads(result.stdout)
                
                # Sort archives by backup time (most recent first)
                archives.sort(key=lambda x: x.get('backup-time', 0), reverse=True)
                
                self.archives_table.setRowCount(len(archives))
                
                # Store the full backup information in the table
                self.archive_data = archives
                
                for i, archive in enumerate(archives):
                    # Get the full snapshot path
                    backup_type = archive.get('backup-type', 'unknown')
                    backup_id = archive.get('backup-id', 'unknown')
                    backup_time_unix = archive.get('backup-time', '')
                    backup_time = datetime.fromtimestamp(backup_time_unix, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    
                    # Format the full snapshot path
                    snapshot_path = f"{backup_type}/{backup_id}/{backup_time}"
                    
                    item = QTableWidgetItem(snapshot_path)
                    # Store the full archive data in the item
                    item.setData(Qt.ItemDataRole.UserRole, archive)
                    self.archives_table.setItem(i, 0, item)
                    
                    # Format size from bytes to human-readable format
                    size_bytes = archive.get('size', 0)
                    size_str = self.format_size(size_bytes)
                    self.archives_table.setItem(i, 1, QTableWidgetItem(size_str))

                    # Format timestamp
                    timestamp = archive.get('backup-time', 'Unknown')
                    try:
                        # Convert Unix timestamp to datetime
                        dt = datetime.fromtimestamp(int(timestamp))
                        timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
                    except (ValueError, TypeError):
                        pass
                    self.archives_table.setItem(i, 2, QTableWidgetItem(timestamp))
                    
                    # Get owner
                    owner = archive.get('owner', 'Unknown')
                    self.archives_table.setItem(i, 3, QTableWidgetItem(owner))
                    
                    # Get verification status
                    status = archive.get('verification', {}).get('state', 'none')
                    status = status.lower()
                    if status not in ['ok', 'none']:
                        status = 'none'
                    self.archives_table.setItem(i, 4, QTableWidgetItem(status))
                    
            else:
                error = result.stderr.strip()
                # logger.error(f"Failed to fetch archives: {error}")
                # QMessageBox.warning(self, "Error", f"Failed to fetch archives: {error}")
        except Exception as e:
            # logger.error(f"Unexpected error when fetching archives: {str(e)}")
            QMessageBox.warning(self, "Error", f"Unexpected error when fetching archives: {str(e)}")

    def restore_archive(self):
        selected_items = self.archives_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Error", "Please select an archive to restore")
            return
        
        # Get the selected row and full snapshot path
        row = selected_items[0].row()
        backup_id = self.archives_table.item(row, 0).text()
        
        # logger.info(f"Starting restore for snapshot: {backup_id}")
        
        try:
            config = self.get_current_config()
            # Ask for restore path
            restore_path = QFileDialog.getExistingDirectory(self, "Select Restore Directory")
            if not restore_path:
                return

            # Get the archive contents
            env = dict(os.environ)
            env['PBS_PASSWORD'] = config['api_key']
            
            cmd = [
                'proxmox-backup-client',
                'snapshot',
                'files',
                backup_id,
                f"--repository", config['repository'],
                '--output-format', 'json'
            ]
            
            # logger.info(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, env=env)
            
            if result.returncode != 0:
                error = result.stderr.strip()
                # logger.error(f"Failed to list snapshot contents: {error}")
                QMessageBox.warning(self, "Error", f"Failed to list snapshot contents: {error}")
                return

            try:
                files_data = json.loads(result.stdout)
                # Filter for .pxar files
                pxar_files = [file["filename"].removesuffix(".didx") for file in files_data if file["filename"].endswith(".pxar.didx")]
                
                if not pxar_files:
                    QMessageBox.warning(self, "Error", "No .pxar.didx files found in backup")
                    return
                    
                # If multiple .pxar files, let user choose
                selected_file = pxar_files[0]
                if len(pxar_files) > 1:
                    item, ok = QInputDialog.getItem(
                        self, 
                        "Select File", 
                        "Choose a file to restore:",
                        pxar_files,
                        0,
                        False
                    )
                    if ok:
                        selected_file = item
                    else:
                        return
            except json.JSONDecodeError:
                # logger.error("Failed to parse snapshot data")
                QMessageBox.warning(self, "Error", "Failed to parse snapshot data")
                return
            
            # Construct the restore command
            cmd = [
                'proxmox-backup-client',
                'restore',
                backup_id,  # Full snapshot path
                selected_file,
                restore_path,
                f"--repository", config['repository']
            ]
            
            # logger.info(f"Running restore command: {' '.join(cmd)}")
            
            # Show progress dialog
            progress = QProgressDialog("Restoring archive...", "Cancel", 0, 0, self)
            progress.setWindowModality(Qt.WindowModality.WindowModal)
            progress.show()
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                env=env
            )
            
            while True:
                output = process.stdout.readline()
                # if output:
                    # logger.info(f"Restore output: {output.strip()}")
                if process.poll() is not None:
                    break
                QApplication.processEvents()
                if progress.wasCanceled():
                    process.terminate()
                    QMessageBox.warning(self, "Cancelled", "Restore operation cancelled")
                    # logger.info("Restore operation cancelled by user")
                    return
            
            progress.close()
            
            if process.returncode == 0:
                # logger.info("Restore completed successfully")
                QMessageBox.information(self, "Success", "Archive restored successfully")
            else:
                error = process.stderr.read()
                # logger.error(f"Restore failed: {error}")
                QMessageBox.warning(self, "Error", f"Failed to restore archive: {error}")
                
        except Exception as e:
            # logger.error(f"Restore failed with unexpected error: {str(e)}")
            QMessageBox.warning(self, "Error", f"Failed to restore archive: {str(e)}")

    def mount_archive(self):
        if self.current_mount:
            QMessageBox.warning(self, "Error", f"An archive is already mounted at {self.current_mount}")
            return

        selected_items = self.archives_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Error", "Please select an archive to mount")
            return
        
        # Get the selected row and full snapshot path
        row = selected_items[0].row()
        backup_id = self.archives_table.item(row, 0).text()
        
        # logger.info(f"Starting mount for snapshot: {backup_id}")
        
        try:
            config = self.get_current_config()
            # Ask for mount point
            mount_path = QFileDialog.getExistingDirectory(self, "Select Mount Directory")
            if not mount_path:
                return

            # Get the archive contents
            env = dict(os.environ)
            env['PBS_PASSWORD'] = config['api_key']
            
            cmd = [
                'proxmox-backup-client',
                'snapshot',
                'files',
                backup_id,  # This is now the full path
                f"--repository", config['repository'],
                '--output-format', 'json'
            ]
            
            # logger.info(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, env=env)
            
            if result.returncode != 0:
                error = result.stderr.strip()
                # logger.error(f"Failed to list snapshot contents: {error}")
                QMessageBox.warning(self, "Error", f"Failed to list snapshot contents: {error}")
                return

            try:
                files_data = json.loads(result.stdout)
                # Filter for .pxar files
                pxar_files = [file["filename"].removesuffix(".didx") for file in files_data if file["filename"].endswith(".pxar.didx")]
                
                if not pxar_files:
                    QMessageBox.warning(self, "Error", "No .pxar.didx files found in backup")
                    return
                    
                # If multiple .pxar files, let user choose
                selected_file = pxar_files[0]
                if len(pxar_files) > 1:
                    item, ok = QInputDialog.getItem(
                        self, 
                        "Select File", 
                        "Choose a file to mount:",
                        pxar_files,
                        0,
                        False
                    )
                    if ok:
                        selected_file = item
                    else:
                        return
            except json.JSONDecodeError:
                # logger.error("Failed to parse snapshot data")
                QMessageBox.warning(self, "Error", "Failed to parse snapshot data")
                return
            
            # Construct the mount command
            cmd = [
                'proxmox-backup-client',
                'mount',
                backup_id,  # Full snapshot path
                selected_file,
                mount_path,
                f"--repository", config['repository']
            ]
            
            # logger.info(f"Running mount command: {' '.join(cmd)}")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                env=env
            )
            
            output, error = process.communicate()
            
            if process.returncode == 0:
                success_msg = f"Archive mounted successfully at {mount_path}"
                # logger.info(success_msg)
                QMessageBox.information(
                    self, 
                    "Success", 
                    success_msg + "\n\nNote: The mount will be automatically unmounted when you close the application."
                )
                # On successful mount, store the mount point:
                self.current_mount = mount_path
                # Update mount status in UI
                self.update_mount_status()
            else:
                # logger.error(f"Mount failed: {error}")
                QMessageBox.warning(self, "Error", f"Failed to mount archive: {error}")
                
        except Exception as e:
            # logger.error(f"Mount failed with unexpected error: {str(e)}")
            QMessageBox.warning(self, "Error", f"Failed to mount archive: {str(e)}")

    def unmount_current(self):
        if not self.current_mount:
            QMessageBox.information(self, "Info", "No archive is currently mounted")
            return

        try:
            cmd = ['fusermount', '-u', self.current_mount]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                QMessageBox.information(self, "Success", f"Archive unmounted successfully from {self.current_mount}")
                self.current_mount = None
                self.update_mount_status()
            else:
                QMessageBox.warning(self, "Error", f"Failed to unmount archive: {result.stderr}")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Failed to unmount archive: {str(e)}")

    def update_mount_status(self):
        if hasattr(self, 'mount_status_label'):
            status = f"Currently mounted at: {self.current_mount}" if self.current_mount else "No archive currently mounted"
            self.mount_status_label.setText(status)

    def delete_archive(self):
        selected_items = self.archives_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Error", "Please select an archive to delete")
            return
        
        # Get the selected row and full snapshot path
        row = selected_items[0].row()
        backup_id = self.archives_table.item(row, 0).text()
        
        # Confirm deletion
        reply = QMessageBox.question(
            self, 
            "Confirm Deletion",
            f"Are you sure you want to delete the archive:\n{backup_id}\n\nThis action cannot be undone!",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                config = self.get_current_config()
                env = dict(os.environ)
                env['PBS_PASSWORD'] = config['api_key']
                
                cmd = [
                    'proxmox-backup-client',
                    'snapshot',
                    'forget',
                    backup_id,
                    f"--repository", config['repository']
                ]
                
                # logger.info(f"Running delete command: {' '.join(cmd)}")
                result = subprocess.run(cmd, capture_output=True, text=True, env=env)
                
                if result.returncode == 0:
                    # logger.info("Archive deleted successfully")
                    QMessageBox.information(self, "Success", "Archive deleted successfully")
                    self.refresh_archives()  # Refresh the archives list
                else:
                    error = result.stderr.strip()
                    # logger.error(f"Failed to delete archive: {error}")
                    QMessageBox.warning(self, "Error", f"Failed to delete archive: {error}")
                    
            except Exception as e:
                # logger.error(f"Delete failed with unexpected error: {str(e)}")
                QMessageBox.warning(self, "Error", f"Failed to delete archive: {str(e)}")

    def format_size(self, size_bytes):
        """Convert bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"

def main():
    app = QApplication(sys.argv)
    window = ProxmoxBackupGUI()
    window.show()
    sys.exit(app.exec())

if __name__ == '__main__':
    main()