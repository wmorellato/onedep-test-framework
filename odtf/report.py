"""
Test Report Generator Module for ODTF

This module provides functionality to generate HTML test reports from test entries
using Jinja2 templates and Bootstrap for styling.
"""

import os
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from jinja2 import Environment, FileSystemLoader

from odtf.config import Config
from odtf.models import TestEntry, TestReport, TaskStatus, CompareFilesTask, CompareReposTask

logger = logging.getLogger(__name__)


class TestReportGenerator:
    """Generates HTML test reports from test entries using Bootstrap templates"""
    
    def __init__(self, config: Config, template_dir: str = "templates", output_dir: str = "reports"):
        """
        Initialize the report generator
        
        Args:
            template_dir: Directory containing Jinja2 templates
            output_dir: Directory where reports will be generated
        """
        self.config = config
        self.template_dir = Path(template_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Setup Jinja2 environment
        self.env = Environment(
            loader=FileSystemLoader(self.template_dir),
            autoescape=True,
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Add custom filters
        self._setup_template_filters()

    def _setup_template_filters(self):
        """Setup custom Jinja2 filters for the templates"""
        
        def format_datetime(dt, format_str='%Y-%m-%d %H:%M:%S'):
            """Format datetime objects"""
            if dt is None:
                return "N/A"
            return dt.strftime(format_str)
        
        def task_status_class(status):
            """Convert task status to CSS class"""
            mapping = {
                TaskStatus.SUCCESS: 'success',
                TaskStatus.FAILED: 'failed',
                TaskStatus.WARNING: 'warning',
                TaskStatus.PENDING: 'pending'
            }
            return mapping.get(status, 'pending')
        
        def task_status_icon(status):
            """Convert task status to Bootstrap icon class"""
            mapping = {
                TaskStatus.SUCCESS: 'bi bi-check-circle-fill text-success',
                TaskStatus.FAILED: 'bi bi-x-circle-fill text-danger',
                TaskStatus.WARNING: 'bi bi-exclamation-triangle-fill text-warning',
                TaskStatus.PENDING: 'bi bi-clock text-muted'
            }
            return mapping.get(status, 'bi bi-clock text-muted')
        
        # Register filters properly
        self.env.filters['format_datetime'] = format_datetime
        self.env.filters['task_status_class'] = task_status_class
        self.env.filters['task_status_icon'] = task_status_icon
    
    def generate_log_filename(self, dep_id: str) -> str:
        """Generate a standardized log filename for a deposition ID"""
        return f"logs/{dep_id}.log"
    
    def prepare_test_entries_for_report(self, 
                                       test_entries: List[TestEntry], 
                                       log_dir: Optional[str] = None) -> List[TestEntry]:
        """
        Prepare test entries for report generation by setting log file paths
        
        Args:
            test_entries: List of test entries
            log_dir: Directory containing log files (relative to report output)
            
        Returns:
            List of test entries with log file paths set
        """
        prepared_entries = []
        
        for entry in test_entries:
            # Create a copy to avoid modifying the original
            prepared_entry = TestEntry(
                dep_id=entry.dep_id,
                copy_dep_id=entry.copy_dep_id,
                tasks=entry.tasks,
                log_file=entry.log_file
            )
            
            # Set log file path if not already set
            if not prepared_entry.log_file and log_dir:
                prepared_entry.log_file = f"{log_dir}/{entry.dep_id}.log"
            elif not prepared_entry.log_file:
                prepared_entry.log_file = self.generate_log_filename(entry.dep_id)
            
            prepared_entries.append(prepared_entry)
        
        return prepared_entries
    
    def create_test_report(self, 
                          test_entries: List[TestEntry], 
                          title: str = "ODTF Test Report",
                          log_dir: Optional[str] = "logs") -> TestReport:
        """
        Create a TestReport object from test entries
        
        Args:
            test_entries: List of test entries
            title: Report title
            log_dir: Directory containing log files
            
        Returns:
            TestReport object ready for template rendering
        """
        prepared_entries = self.prepare_test_entries_for_report(test_entries, log_dir)
        
        report = TestReport(
            base_url=self.config.report.get("depui_url"),
            test_entries=prepared_entries,
            title=title,
            generation_time=datetime.now()
        )
        
        return report
    
    def generate_html_report(self, 
                            test_report: TestReport,
                            template_name: str = "test_report.html",
                            output_filename: Optional[str] = None) -> str:
        """
        Generate HTML report from TestReport object
        
        Args:
            test_report: TestReport object containing all data
            template_name: Name of the Jinja2 template file
            output_filename: Output filename (auto-generated if None)
            
        Returns:
            Path to the generated HTML file
        """
        try:
            template = self.env.get_template(template_name)
        except Exception as e:
            logger.error(f"Failed to load template {template_name}: {e}")
            raise
        
        # Generate output filename if not provided
        if not output_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"test_report_{timestamp}.html"
        
        output_path = self.output_dir / output_filename
        
        try:
            # Render template
            html_content = template.render(report=test_report)
            
            # Write to file
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            logger.info(f"Report generated successfully: {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Failed to generate report: {e}")
            raise
    
    def generate_report_from_entries(self, 
                                    test_entries: List[TestEntry],
                                    title: str = "ODTF Test Report",
                                    template_name: str = "test_report.html",
                                    output_filename: Optional[str] = None,
                                    log_dir: Optional[str] = "logs") -> str:
        """
        Convenience method to generate report directly from test entries
        
        Args:
            test_entries: List of test entries
            title: Report title
            template_name: Name of the Jinja2 template file
            output_filename: Output filename (auto-generated if None)
            log_dir: Directory containing log files
            
        Returns:
            Path to the generated HTML file
        """
        test_report = self.create_test_report(test_entries, title, log_dir)
        return self.generate_html_report(test_report, template_name, output_filename)


def update_task_status_from_entry_status(test_entry: TestEntry, entry_status) -> None:
    """
    Update task statuses based on EntryStatus information
    This is a helper function to bridge between your existing EntryStatus system
    and the new Task status tracking
    
    Args:
        test_entry: TestEntry to update
        entry_status: EntryStatus object from your existing system
    """
    if not test_entry.tasks:
        return
    
    # Map entry status to task status
    status_mapping = {
        "finished": TaskStatus.SUCCESS,
        "failed": TaskStatus.FAILED,
        "warning": TaskStatus.WARNING,
        "working": TaskStatus.PENDING,
        "pending": TaskStatus.PENDING
    }
    
    # Update all tasks based on overall entry status
    task_status = status_mapping.get(entry_status.status, TaskStatus.PENDING)
    
    for task in test_entry.tasks:
        # Only update if task is still pending
        if task.status == TaskStatus.PENDING:
            task.status = task_status
            task.error_message = entry_status.message if task_status == TaskStatus.FAILED else None
            task.execution_time = datetime.now()


def update_compare_files_task_from_results(task: CompareFilesTask, 
                                         comparison_results: Dict[str, Dict]) -> None:
    """
    Update CompareFilesTask rules based on comparison results
    
    Args:
        task: CompareFilesTask to update
        comparison_results: Dictionary mapping rule names to results
                          Format: {rule_name: {'success': bool, 'error': str}}
    """
    for rule in task.rules:
        if rule.name in comparison_results:
            result = comparison_results[rule.name]
            if result.get('success', False):
                rule.status = TaskStatus.SUCCESS
                rule.error_message = None
            else:
                rule.status = TaskStatus.FAILED
                rule.error_message = result.get('error', 'Comparison failed')
        else:
            # Rule wasn't processed - mark as pending
            rule.status = TaskStatus.PENDING


def update_compare_repos_task_from_results(task: CompareReposTask, 
                                         comparison_results: Dict[str, Dict]) -> None:
    """
    Update CompareReposTask rules based on comparison results
    
    Args:
        task: CompareReposTask to update
        comparison_results: Dictionary mapping dep_id to results
                          Format: {dep_id: {'success': bool, 'error': str}}
    """
    if "repository" in comparison_results:
        result = comparison_results["repository"] # if I make more than one comparison, this will break
        if result.get('success', False):
            task.status = TaskStatus.SUCCESS
            task.error_message = None
        else:
            task.status = TaskStatus.FAILED
            task.error_message = result.get('error', 'Repository comparison failed')
    else:
        # Dep_id wasn't processed - mark as pending
        task.status = TaskStatus.PENDING


# Example usage and integration helpers
class TestReportIntegration:
    """
    Helper class to integrate report generation with your existing CLI pipeline
    """
    
    def __init__(self, report_generator: TestReportGenerator):
        self.report_generator = report_generator
        self.comparison_results = {}  # Store comparison results per entry
    
    def track_comparison_result(self, dep_id: str, rule_name: str, success: bool, error: str = None):
        """Track comparison results for later report generation"""
        if dep_id not in self.comparison_results:
            self.comparison_results[dep_id] = {}
        
        self.comparison_results[dep_id][rule_name] = {
            'success': success,
            'error': error
        }
    
    def update_test_entries_from_status_manager(self, 
                                               test_entries: List[TestEntry], 
                                               status_manager) -> List[TestEntry]:
        """
        Update test entries with information from your StatusManager
        
        Args:
            test_entries: List of test entries to update
            status_manager: Your existing StatusManager instance
            
        Returns:
            Updated test entries with current status information
        """
        updated_entries = []
        
        for entry in test_entries:
            # Get status from status manager
            try:
                entry_status = status_manager.get_status(entry)
                
                # Update task statuses
                update_task_status_from_entry_status(entry, entry_status)
                
                # Update copy_dep_id if available
                if entry_status.copy_dep_id and entry_status.copy_dep_id != "?":
                    entry.copy_dep_id = entry_status.copy_dep_id
                
                # Update comparison results for compare_files tasks
                if entry.dep_id in self.comparison_results:
                    for task in entry.tasks:
                        if isinstance(task, CompareFilesTask):
                            update_compare_files_task_from_results(
                                task, 
                                self.comparison_results[entry.dep_id]
                            )
                        elif isinstance(task, CompareReposTask):
                            update_compare_repos_task_from_results(
                                task,
                                self.comparison_results[entry.dep_id]
                            )
                
                updated_entries.append(entry)
                
            except KeyError:
                logger.warning(f"No status found for entry {entry.dep_id}")
                updated_entries.append(entry)
        
        return updated_entries
    
    def generate_final_report(self, 
                             test_entries: List[TestEntry], 
                             status_manager,
                             output_filename: Optional[str] = None) -> str:
        """
        Generate the final test report with all current status information
        
        Args:
            test_entries: List of test entries
            status_manager: Your existing StatusManager instance
            output_filename: Optional custom output filename
            
        Returns:
            Path to generated report
        """
        # Update entries with latest status
        updated_entries = self.update_test_entries_from_status_manager(
            test_entries, status_manager
        )
        
        # Generate report
        return self.report_generator.generate_report_from_entries(
            updated_entries,
            title="ODTF Test Execution Report",
            output_filename=output_filename
        )


if __name__ == "__main__":
    # Example usage
    from models import TestEntry, UploadTask, CompareFilesTask, TaskType
    
    # Create sample test entries
    test_entries = [
        TestEntry(
            dep_id="D_8233000125",
            copy_dep_id="D_8233000999",
            tasks=[
                UploadTask(),
                CompareFilesTask(source="copy", rules=["model.pdb", "em-volume.map"])
            ]
        ),
        TestEntry(
            dep_id="D_8233000126", 
            tasks=[
                UploadTask(),
                CompareFilesTask(source="copy", rules=["model.pdbx"])
            ]
        )
    ]
    
    # Set some example statuses
    test_entries[0].tasks[0].status = TaskStatus.SUCCESS
    test_entries[0].tasks[1].status = TaskStatus.FAILED
    test_entries[0].tasks[1].error_message = "File comparison failed"
    
    test_entries[1].tasks[0].status = TaskStatus.SUCCESS
    test_entries[1].tasks[1].status = TaskStatus.SUCCESS
    
    # Generate report
    generator = TestReportGenerator()
    report_path = generator.generate_report_from_entries(
        test_entries,
        title="Sample ODTF Test Report"
    )
    
    print(f"Sample report generated: {report_path}")