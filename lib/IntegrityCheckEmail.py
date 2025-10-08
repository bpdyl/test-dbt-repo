"""##################################################################
# Script      : IntegrityCheckEmail.py

# Description : This module handles email notifications for data integrity checks
# Modifications
# 25/06/2025  : Robling  : Initial Script
# 06/10/2025  : Robling  : Added severity logic
#####################################################################"""


from lib.SendGridEmailModule import SendGridEmailModule

class IntegrityCheckEmail:
    """This Class is used to send email notifications for integrity check results"""
    
    def __init__(self, var):
        # Get configuration values
        self.INTEGRITY_DOMAIN = var.get('INTEGRITY_DOMAIN')
        self.DASHBOARD = var.get('INTEGRITY_DASHBOARD')
        self.EXEC_ENV = var.get('EXEC_ENV')
        self.INTEGRITY_EMAIL_LIST = var.get('RECON_EMAIL_LIST')
        
        self.style = """<html>
            <head>
                <style>
                    table {
                        border-collapse: collapse;
                        font-family: Tahoma, Geneva, sans-serif;
                        text-align: left;
                    }

                    table thead tr {
                        white-space: nowrap;
                        background-color: #f8f9fa;
                        color: black;
                        font-size: 13px;
                        border: 1px solid #54585d;
                        padding: 10px;
                        font-weight: bold;
                    }

                    table tbody td {
                        white-space: nowrap;
                        color: black;
                        background-color: white;
                        border: 1px solid black;
                        padding: 10px;
                    }
                    
                    .pass {
                        background-color: #d4edda !important;
                        color: #155724;
                    }
                    
                    .fail {
                        background-color: #f8d7da !important;
                        color: #721c24;
                    }
                    
                    .critical {
                        
                        color: #721c24;
                        font-weight: bold;
                    }
                    
                    .warning {
                        font-weight: bold;
                        color: #b78905;
                    }
                    
                    .informational {
                        
                        color: #0c5460;
                    }
                </style>
            </head>"""

    def _get_email_footer(self):
        """Common email footer for all integrity check emails"""
        return f'<br/><br/>' \
               f'*** Please do not reply to this message because it will end up in deep ' \
               f'abyss where it will never be read. This is an automated message sent from an unmonitored email ' \
               f'account that is used for outgoing communication only. ***' \
               f'<br/><br/>' \
               f'For support or data requests, please visit ' \
               f'<a target="_blank" href="https://robling.io/askrobling/">https://robling.io/askrobling/</a> ' \
               f'or send a message to ' \
               f'<a href="mailto:askrobling@robling.io">askrobling@robling.io</a>.' \
               f'<br /><br /><br />' \
               f'Best Regards,<br /><strong><label style="color:#00BCD4">' \
               f'Team Robling</label></strong>' \
               f'<br /><a target="_blank" href="https://robling.io">www.robling.io</a>'

    def _create_violations_table(self, violations):
        """Create HTML table for integrity check violations with severity"""
        if not violations:
            return "<p>No violations found.</p>"
        
        # Generate HTML table for violations
        table_html = """<table border="1" class="dataframe">
            <thead>
                <tr style="text-align: left;">
                    <th>Test Description</th>
                    <th style="text-align:right;">Violating Rows</th>
                    <th style="text-align:right;">Total Rows</th>
                    <th style="text-align:right;">Violation %</th>
                    <th>Status</th>
                    <th>Severity</th>
                </tr>
            </thead>
            <tbody>"""
        
        for test_case in violations:
            try:
                violating_rows = int(test_case['integrity_chk_violation_row_cnt'])
            except Exception:
                violating_rows = 0
            try:
                total_rows = int(test_case['row_cnt'])
            except Exception:
                total_rows = 0
            violation_pct = (
                round((violating_rows / total_rows) * 100, 1)
                if total_rows > 0 else 0.0
            )
            
            # Determine row styling based on status and severity
            status_class = "pass" if test_case["pass_fail"] == "Pass" else "fail"
            severity = test_case.get('severity', 'Warning')
            severity_class = severity.lower()
            
            table_html += f"""
        <tr>
            <td>{test_case["test_desc"]}</td>
            <td style="text-align:right;">{violating_rows:,}</td>
            <td style="text-align:right;">{total_rows:,}</td>
            <td style="text-align:right;">{violation_pct:.1f}%</td>
            <td class="{status_class}">{test_case["pass_fail"]}</td>
            <td class="{severity_class}">{severity}</td>
        </tr>"""
        
        table_html += """
            </tbody>
        </table>"""
        return table_html

    def _analyze_failures(self, violations):
        """Analyze failed tests by severity level"""
        failed_tests = [v for v in violations if v["pass_fail"] == "Fail"]
        
        severity_counts = {
            'Critical': [],
            'Warning': [],
            'Informational': []
        }
        
        for test in failed_tests:
            severity = test.get('severity', 'Warning')
            severity_counts[severity].append(test)
        
        return severity_counts

    def _get_email_priority(self, severity_counts):
        """Determine email priority based on severity of failures"""
        if severity_counts['Critical']:
            return 'Critical'
        elif severity_counts['Warning']:
            return 'Warning'
        elif severity_counts['Informational']:
            return 'Informational'
        else:
            return 'Success'

    def send_critical_failure_email(self, integrity_json, severity_counts):
        """Send critical failure email for Critical severity failures"""
        sub_area = integrity_json["sub_area_name"]
        table_name = integrity_json["table_name"]
        business_date = integrity_json["business_date"]
        violations = integrity_json["violations"]
        
        critical_count = len(severity_counts['Critical'])
        warning_count = len(severity_counts['Warning'])
        info_count = len(severity_counts['Informational'])
        
        email_subject = f'FAILURE! :: {self.EXEC_ENV} Critical integrity issues for {sub_area}/{table_name} on {business_date}'
        
        email_message = f'Hi all,<br/><br/>' \
                       f'<strong style="color: #721c24;"> URGENT ACTION REQUIRED </strong><br/><br/>' \
                       f'Robling found <strong style="color: #721c24;">{critical_count} CRITICAL</strong> data integrity issue(s) ' \
                       f'for <strong>{sub_area}/{table_name}</strong> that require immediate attention.<br/><br/>' \
                       f'Summary of failures:<br/>' \
                       f'• <strong style="color: #721c24;">Critical: {critical_count}</strong><br/>' \
                       f'• Warning: {warning_count}<br/>' \
                       f'• Informational: {info_count}<br/><br/>' \
                       f'These critical issues may invalidate reporting and affect downstream processes.<br/><br/>' \
                       f'Detailed results:'
        
        table_html = self._create_violations_table(violations)
        email_message = email_message + self.style + table_html
        
        # Add dashboard link
        email_message = email_message + f'<br/><br/>' \
                       f'View detailed analysis: <a href="https://{self.INTEGRITY_DOMAIN}/{self.DASHBOARD}?Table={table_name}&Date={business_date}&Severity=Critical">Critical Issues Report</a>'
        
        email_message = email_message + self._get_email_footer()
        
        SendGridEmailModule.send_email(email_subject, email_message, self.INTEGRITY_EMAIL_LIST.split(','))

    def send_warning_email(self, integrity_json, severity_counts):
        """Send warning email for Warning severity failures (no Critical failures)"""
        sub_area = integrity_json["sub_area_name"]
        table_name = integrity_json["table_name"]
        business_date = integrity_json["business_date"]
        violations = integrity_json["violations"]
        
        warning_count = len(severity_counts['Warning'])
        info_count = len(severity_counts['Informational'])
        
        email_subject = f'WARNING :: {self.EXEC_ENV} Integrity warnings for {sub_area}/{table_name} on {business_date}'
        
        email_message = f'Hi all,<br/><br/>' \
                       f'Robling found <strong style="color: #856404;">{warning_count} Warning</strong> level integrity issue(s) ' \
                       f'for <strong>{sub_area}/{table_name}</strong>.<br/><br/>' \
                       f'Summary of issues:<br/>' \
                       f'• Warning: {warning_count}<br/>' \
                       f'• Informational: {info_count}<br/><br/>' \
                       f'These issues may impact data quality but don\'t necessarily affect core functionality.<br/><br/>' \
                       f'Detailed results:'
        
        table_html = self._create_violations_table(violations)
        email_message = email_message + self.style + table_html
        
        # Add dashboard link
        email_message = email_message + f'<br/><br/>' \
                       f'View detailed analysis: <a href="https://{self.INTEGRITY_DOMAIN}/{self.DASHBOARD}?Table={table_name}&Date={business_date}&Severity=Warning">Warning Issues Report</a>'
        
        email_message = email_message + self._get_email_footer()
        
        SendGridEmailModule.send_email(email_subject, email_message, self.INTEGRITY_EMAIL_LIST.split(','))

    def send_informational_email(self, integrity_json, severity_counts):
        """Send informational email for Informational severity failures only"""
        sub_area = integrity_json["sub_area_name"]
        table_name = integrity_json["table_name"]
        business_date = integrity_json["business_date"]
        violations = integrity_json["violations"]
        
        info_count = len(severity_counts['Informational'])
        
        email_subject = f'ℹINFO :: {self.EXEC_ENV} Informational notices for {sub_area}/{table_name} on {business_date}'
        
        email_message = f'Hi all,<br/><br/>' \
                       f'Robling found <strong style="color: #0c5460;">{info_count} Informational</strong> notice(s) ' \
                       f'for <strong>{sub_area}/{table_name}</strong>.<br/><br/>' \
                       f'These are tracked for completeness but don\'t impact functionality.<br/><br/>' \
                       f'Detailed results:'
        
        table_html = self._create_violations_table(violations)
        email_message = email_message + self.style + table_html
        
        # Add dashboard link
        email_message = email_message + f'<br/><br/>' \
                       f'View detailed analysis: <a href="https://{self.INTEGRITY_DOMAIN}/{self.DASHBOARD}?Table={table_name}&Date={business_date}&Severity=Informational">Full Report</a>'
        
        email_message = email_message + self._get_email_footer()
        
        SendGridEmailModule.send_email(email_subject, email_message, self.INTEGRITY_EMAIL_LIST.split(','))

    def send_success_email(self, integrity_json):
        """Send success email when all integrity checks pass"""
        sub_area = integrity_json["sub_area_name"]
        table_name = integrity_json["table_name"]
        business_date = integrity_json["business_date"]
        violations = integrity_json["violations"]
        
        email_subject = f'SUCCESS :: {self.EXEC_ENV} All integrity checks passed for {table_name} in {sub_area} on {business_date}'
        
        email_message = f'Hi all,<br/><br/>' \
                       f'Robling ran data integrity checks for <strong>{sub_area}/{table_name}</strong>. ' \
                       f'<strong style="color: #155724;">All checks passed successfully!</strong><br/><br/>' \
                       f'Detailed results:'
        
        table_html = self._create_violations_table(violations)
        email_message = email_message + self.style + table_html
        
        # Add dashboard link
        email_message = email_message + f'<br/><br/>' \
                       f'View full report: <a href="https://{self.INTEGRITY_DOMAIN}/{self.DASHBOARD}?Table={table_name}&Date={business_date}">Success Report</a>'
        
        email_message = email_message + self._get_email_footer()
        
        SendGridEmailModule.send_email(email_subject, email_message, self.INTEGRITY_EMAIL_LIST.split(','))

    def send_integrity_email(self, integrity_json):
        """Main method to determine which type of email to send based on severity analysis"""
        
        # Sort violations - failures first by severity (Critical > Warning > Informational), 
        # then by violation count descending
        def sort_key(violation):
            severity_priority = {
                'Critical': 1,
                'Warning': 2, 
                'Informational': 3
            }
            severity = violation.get('severity', 'Warning')
            violation_count = int(violation.get('integrity_chk_violation_row_cnt', 0))
            is_failure = 1 if violation['pass_fail'] == 'Fail' else 2
            
            return (is_failure, severity_priority.get(severity, 2), -violation_count)
        
        violations = sorted(integrity_json['violations'], key=sort_key)
        integrity_json['violations'] = violations
        
        # Analyze failures by severity
        severity_counts = self._analyze_failures(violations)
        email_priority = self._get_email_priority(severity_counts)
        
        # Send appropriate email based on highest severity failure
        if email_priority == 'Critical':
            print(f"Critical integrity failures found - sending critical alert email")
            self.send_critical_failure_email(integrity_json, severity_counts)
        elif email_priority == 'Warning':
            print(f"Warning level integrity issues found - sending warning email")
            self.send_warning_email(integrity_json, severity_counts)
        elif email_priority == 'Informational':
            print(f"Informational integrity notices found - sending info email")
            self.send_informational_email(integrity_json, severity_counts)
        else:
            print("All integrity checks passed - sending success email")
            self.send_success_email(integrity_json)