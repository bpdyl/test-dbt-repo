"""##################################################################
# Script      : ReconciliationEmail.py
# Description : This module has functions related to sending email notifications for reconciliation check
#               between any two given data source (Staging view, Landing, DWH, DM).
#               The functions present in this module sends out email notifications based on the result obtained.
#               Notifications are sent for success, warning, failure and no data present.
#
# Modifications
# 02/07/2024  : Robling  : Updated header description
# 24/01/2023  : Robling  : Initial Script
#####################################################################"""

import json
from lib.SendGridEmailModule import SendGridEmailModule
from lib.Formatter import Formatter



class ReconciliationEmail:
    ### This Class is used to send email notifications for reconciliation check
    def __init__(self,var):
        self.RECON_DOMAIN = var.get('RECON_DOMAIN')
        self.DASHBOARD = var.get('DASHBOARD')
        self.EXEC_ENV = var.get('EXEC_ENV')
        self.RECON_EMAIL_LIST = var.get('RECON_EMAIL_LIST')
        self.style = """<html>
                    <head>
                    <style>
                    table {
                        border-collapse: collapse;
                        font-family: Tahoma, Geneva, sans-serif;
                        text-align: right;
                    }
                    table thead tr {
                        white-space:nowrap;
                        background-color: white;
                        color: black;                      
                        font-size: 13px;
                        border: 1px solid #54585d;
                        padding: 10px;
                    }
                    table tbody td {
                        white-space:nowrap;
                        color: black;
                        background-color: white;
                        border: 1px solid black;
                        padding: 10px;
                    }                 
                    </style>
                    </head>"""

    def send_no_data_email(self, json_result):
        ### This function sends failure email to recon recepients if validation fails
        curr_date = json_result["current_day"]
        fact_typ = json_result["fact_type"]
        checkpoint = json_result["checkpoint"]
        src_sys1 = checkpoint[json_result["source_systems"]["system1"]]
        src_sys2 = checkpoint[json_result["source_systems"]["system2"]]
        email_subject = f'FAILURE! :: {self.EXEC_ENV} The load for {fact_typ} has failed on {curr_date} due to no data available for reconciliation checkpoint.'
        email_message = f'Hi all,' \
                        f'<br/>' \
                        f'<br/>' \
                        f'Robling ran a reconciliation checkpoint for {fact_typ} between {src_sys1} and {src_sys2}. ' \
                        f'No data was found for reconciliation.' \
                        f'<br/><br/>Here are the numbers.'

        table_html = f"""<table border="1" class="dataframe">
                                 <thead>
                                   <tr style="text-align: right;">
                                     <th>Batch Date</th>
                                     <th>Checkpoint</th>
                                     <th>Total Retail</th>
                                     <th>Total Cost</th>
                                     <th>Total Units</th>
                                     <th>Diff. Retail</th>
                                     <th>Diff. Cost</th>
                                     <th>Diff. Units</th>
                                     <th>Var. % Retail</th>
                                     <th>Var. % Cost</th>
                                     <th>Var. % Units</th>
                                   </tr>
                                 </thead>
                                 <tbody>
                                   <tr>
                                     <td>{curr_date}</td>
                                     <td>{src_sys1}</td>
                                     <td>{json_result["results"]["rtl"]["system1_value"]}</td>
                                     <td>{json_result["results"]["cst"]["system1_value"]}</td>
                                     <td>{json_result["results"]["unt"]["system1_value"]}</td>
                                     <td></td>
                                     <td></td>
                                     <td></td>
                                     <td></td>
                                     <td></td>
                                     <td></td>                                 
                                   </tr>
                                   <tr>
                                     <td>{curr_date}</td>
                                     <td>{src_sys2}</td>
                                     <td>{json_result["results"]["rtl"]["system2_value"]}</td>
                                     <td>{json_result["results"]["cst"]["system2_value"]}</td>
                                     <td>{json_result["results"]["unt"]["system2_value"]}</td>
                                     <td>{json_result["results"]["rtl"]["difference"]}</td>
                                     <td>{json_result["results"]["cst"]["difference"]}</td>
                                     <td>{json_result["results"]["unt"]["difference"]}</td>
                                     <td>{json_result["results"]["rtl"]["variance_percent"]}</td>
                                     <td>{json_result["results"]["cst"]["variance_percent"]}</td>
                                     <td>{json_result["results"]["unt"]["variance_percent"]}</td>                                 
                                   </tr>
                                 </tbody>
                               </table>"""
        email_message = email_message \
                        + self.style + table_html
        email_message = email_message + \
                        f'<br/><br/>' \
                        f'Here is a <a href="https://{self.RECON_DOMAIN}/{self.DASHBOARD}?Metric={fact_typ}&Batch+Date={curr_date}"" > report </a> if you would like more details'
        email_footer = f'<br/><br/>' \
                       f'*** Please do not reply to this message because it will end up in deep ' \
                       f'abyss where it will never be read. This is an automated message sent from an unmonitored email ' \
                       f'account that is used for outgoing communication only. ***' \
                       f'<br/><br/>' \
                       f'For support or data requests, please visit ' \
                       f'<a target="_blank" href="https://robling.io/askrobling/">https://robling.io/askrobling/</a> ' \
                       f'or send a message to ' \
                       f'<a href="mailto:askrobling@robling.io">askrobling@robling.io</a>.' \
                       f'<br/><br/><br/>' \
                       f'Best Regards,<br/><strong><label style="color:#00BCD4">' \
                       f'Team Robling</label></strong>' \
                       f'<br/><a target="_blank" href="https://robling.io">www.robling.io</a>'

        email_message = email_message + email_footer
        SendGridEmailModule.send_email(email_subject, email_message,
                                      self.RECON_EMAIL_LIST.split(','))

    def send_no_data_warning_email(self, json_result):
        """Send a warning email when no data is available but the load is configured to skip."""
        curr_date = json_result["current_day"]
        fact_typ = json_result["fact_type"]
        checkpoint = json_result["checkpoint"]
        warning_message = json_result.get("warning_message") or "No data was available for reconciliation."
        src_sys1 = checkpoint[json_result["source_systems"]["system1"]]
        src_sys2 = checkpoint[json_result["source_systems"]["system2"]]
        email_subject = f'Warning! :: {self.EXEC_ENV} The load for {fact_typ} was skipped on {curr_date} because no data was available for reconciliation.'
        email_message = f'Hi all,' \
                        f'<br/>' \
                        f'<br/>' \
                        f'{warning_message} The downstream models were skipped as per configuration.' \
                        f'<br/><br/>Here are the checkpoint numbers (all zeros indicate no rows were processed).'

        table_html = f"""<table border="1" class="dataframe">
                                 <thead>
                                   <tr style="text-align: right;">
                                     <th>Batch Date</th>
                                     <th>Checkpoint</th>
                                     <th>Total Retail</th>
                                     <th>Total Cost</th>
                                     <th>Total Units</th>
                                     <th>Diff. Retail</th>
                                     <th>Diff. Cost</th>
                                     <th>Diff. Units</th>
                                     <th>Var. % Retail</th>
                                     <th>Var. % Cost</th>
                                     <th>Var. % Units</th>
                                   </tr>
                                 </thead>
                                 <tbody>
                                   <tr>
                                     <td>{curr_date}</td>
                                     <td>{src_sys1}</td>
                                     <td>{json_result["results"]["rtl"]["system1_value"]}</td>
                                     <td>{json_result["results"]["cst"]["system1_value"]}</td>
                                     <td>{json_result["results"]["unt"]["system1_value"]}</td>
                                     <td>{json_result["results"]["rtl"]["difference"]}</td>
                                     <td>{json_result["results"]["cst"]["difference"]}</td>
                                     <td>{json_result["results"]["unt"]["difference"]}</td>
                                     <td>{json_result["results"]["rtl"]["variance_percent"]}</td>
                                     <td>{json_result["results"]["cst"]["variance_percent"]}</td>
                                     <td>{json_result["results"]["unt"]["variance_percent"]}</td>
                                   </tr>
                                   <tr>
                                     <td>{curr_date}</td>
                                     <td>{src_sys2}</td>
                                     <td>{json_result["results"]["rtl"]["system2_value"]}</td>
                                     <td>{json_result["results"]["cst"]["system2_value"]}</td>
                                     <td>{json_result["results"]["unt"]["system2_value"]}</td>
                                     <td>{json_result["results"]["rtl"]["difference"]}</td>
                                     <td>{json_result["results"]["cst"]["difference"]}</td>
                                     <td>{json_result["results"]["unt"]["difference"]}</td>
                                     <td>{json_result["results"]["rtl"]["variance_percent"]}</td>
                                     <td>{json_result["results"]["cst"]["variance_percent"]}</td>
                                     <td>{json_result["results"]["unt"]["variance_percent"]}</td>
                                   </tr>
                                 </tbody>
                               </table>"""
        email_message = email_message + self.style + table_html
        email_message = email_message + \
                        f'<br/><br/>' \
                        f'Here is a <a href="https://{self.RECON_DOMAIN}/{self.DASHBOARD}?Metric={fact_typ}&Batch+Date={curr_date}"" > report </a> if you would like more details'
        email_footer = f'<br/><br/>' \
                       f'*** Please do not reply to this message because it will end up in deep ' \
                       f'abyss where it will never be read. This is an automated message sent from an unmonitored email ' \
                       f'account that is used for outgoing communication only. ***' \
                       f'<br/><br/>' \
                       f'For support or data requests, please visit ' \
                       f'<a target="_blank" href="https://robling.io/askrobling/">https://robling.io/askrobling/</a> ' \
                       f'or send a message to ' \
                       f'<a href="mailto:askrobling@robling.io">askrobling@robling.io</a>.' \
                       f'<br/><br/><br/>' \
                       f'Best Regards,<br/><strong><label style="color:#00BCD4">' \
                       f'Team Robling</label></strong>' \
                       f'<br/><a target="_blank" href="https://robling.io">www.robling.io</a>'

        email_message = email_message + email_footer
        SendGridEmailModule.send_email(email_subject, email_message,
                                      self.RECON_EMAIL_LIST.split(','))

    def send_failure_email(self, json_result):
        ### This function sends failure email to recon recepients if validation fails
        curr_date = json_result["current_day"]
        fact_typ = json_result["fact_type"]
        checkpoint = json_result["checkpoint"]
        src_sys1 = checkpoint[json_result["source_systems"]["system1"]]
        src_sys2 = checkpoint[json_result["source_systems"]["system2"]]
        
        email_subject = f'FAILURE! :: {self.EXEC_ENV}  The load for {fact_typ} has failed on {curr_date} due to an unacceptable difference in a reconciliation checkpoint.'
        email_message = f'Hi all,'\
                        f'<br/>'\
                        f'<br/>'\
                        f'Robling ran a reconciliation checkpoint for {fact_typ} between {src_sys1} and {src_sys2}. '\
                        f'A data difference was found that was out of tolerance and the daily load was halted.'\
                        f'<br/><br/>Here are the numbers.'

        table_html = f"""<table border="1" class="dataframe">
                              <thead>
                                <tr style="text-align: right;">
                                  <th>Batch Date</th>
                                  <th>Checkpoint</th>
                                  <th>Total Retail</th>
                                  <th>Total Cost</th>
                                  <th>Total Units</th>
                                  <th>Diff. Retail</th>
                                  <th>Diff. Cost</th>
                                  <th>Diff. Units</th>
                                  <th>Var. % Retail</th>
                                  <th>Var. % Cost</th>
                                  <th>Var. % Units</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr>
                                  <td>{curr_date}</td>
                                  <td>{src_sys1}</td>
                                  <td>{json_result["results"]["rtl"]["system1_value"]}</td>
                                  <td>{json_result["results"]["cst"]["system1_value"]}</td>
                                  <td>{json_result["results"]["unt"]["system1_value"]}</td>
                                  <td></td>
                                  <td></td>
                                  <td></td>
                                  <td></td>
                                  <td></td>
                                  <td></td>                                 
                                </tr>
                                <tr>
                                  <td>{curr_date}</td>
                                  <td>{src_sys2}</td>
                                  <td>{json_result["results"]["rtl"]["system2_value"]}</td>
                                  <td>{json_result["results"]["cst"]["system2_value"]}</td>
                                  <td>{json_result["results"]["unt"]["system2_value"]}</td>
                                  <td>{json_result["results"]["rtl"]["difference"]}</td>
                                  <td>{json_result["results"]["cst"]["difference"]}</td>
                                  <td>{json_result["results"]["unt"]["difference"]}</td>
                                  <td>{json_result["results"]["rtl"]["variance_percent"]}</td>
                                  <td>{json_result["results"]["cst"]["variance_percent"]}</td>
                                  <td>{json_result["results"]["unt"]["variance_percent"]}</td>                                 
                                </tr>
                              </tbody>
                            </table>"""
        email_message = email_message \
                        + self.style + table_html
        email_message = email_message + \
                        f'<br/><br/>'\
                        f'Here is a <a href="https://{self.RECON_DOMAIN}/{self.DASHBOARD}?Metric={fact_typ}&Batch+Date={curr_date}"" > report </a> if you would like more details'
        email_footer = f'<br/><br/>' \
                       f'*** Please do not reply to this message because it will end up in deep ' \
                       f'abyss where it will never be read. This is an automated message sent from an unmonitored email ' \
                       f'account that is used for outgoing communication only. ***' \
                       f'<br/><br/>' \
                       f'For support or data requests, please visit ' \
                       f'<a target="_blank" href="https://robling.io/askrobling/">https://robling.io/askrobling/</a> ' \
                       f'or send a message to ' \
                       f'<a href="mailto:askrobling@robling.io">askrobling@robling.io</a>.' \
                       f'<br/><br/><br/>' \
                       f'Best Regards,<br/><strong><label style="color:#00BCD4">' \
                       f'Team Robling</label></strong>' \
                       f'<br/><a target="_blank" href="https://robling.io">www.robling.io</a>'

        email_message = email_message + email_footer
        SendGridEmailModule.send_email(email_subject, email_message,
                                      self.RECON_EMAIL_LIST.split(','))

    def send_warning_email(self, json_result):
        ### This function sends warning email to recon recepients if validation fails within a range
        curr_date = json_result["current_day"]
        fact_typ = json_result["fact_type"]
        checkpoint = json_result["checkpoint"]
        src_sys1 = checkpoint[json_result["source_systems"]["system1"]]
        src_sys2 = checkpoint[json_result["source_systems"]["system2"]]
        
        retail_diff = json_result["results"]["rtl"]["difference"]
        email_subject = f'Warning! :: {self.EXEC_ENV}  {fact_typ} had a {retail_diff} discrepancy on a reconciliation checkpoint on {curr_date}.'
        email_message = f'Hi all,'\
                        f'<br/>'\
                        f'<br/>'\
                        f'Robling ran a reconciliation checkpoint for {fact_typ} between {src_sys1} and {src_sys2}. '\
                        f'A data {retail_diff} discrepancy was found that was within tolerance and the daily load was allowed to proceed.'\
                        f'<br/><br/>Here are the numbers.'

        table_html = f"""<table border="1" class="dataframe">
                              <thead>
                                <tr style="text-align: right;">
                                  <th>Batch Date</th>
                                  <th>Checkpoint</th>
                                  <th>Total Retail</th>
                                  <th>Total Cost</th>
                                  <th>Total Units</th>
                                  <th>Diff. Retail</th>
                                  <th>Diff. Cost</th>
                                  <th>Diff. Units</th>
                                  <th>Var. % Retail</th>
                                  <th>Var. % Cost</th>
                                  <th>Var. % Units</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr>
                                  <td>{curr_date}</td>
                                  <td>{src_sys1}</td>
                                  <td>{json_result["results"]["rtl"]["system1_value"]}</td>
                                  <td>{json_result["results"]["cst"]["system1_value"]}</td>
                                  <td>{json_result["results"]["unt"]["system1_value"]}</td>
                                  <td></td>
                                  <td></td>
                                  <td></td>
                                  <td></td>
                                  <td></td>
                                  <td></td>                                 
                                </tr>
                                <tr>
                                  <td>{curr_date}</td>
                                  <td>{src_sys2}</td>
                                  <td>{json_result["results"]["rtl"]["system2_value"]}</td>
                                  <td>{json_result["results"]["cst"]["system2_value"]}</td>
                                  <td>{json_result["results"]["unt"]["system2_value"]}</td>
                                  <td>{json_result["results"]["rtl"]["difference"]}</td>
                                  <td>{json_result["results"]["cst"]["difference"]}</td>
                                  <td>{json_result["results"]["unt"]["difference"]}</td>
                                  <td>{json_result["results"]["rtl"]["variance_percent"]}</td>
                                  <td>{json_result["results"]["cst"]["variance_percent"]}</td>
                                  <td>{json_result["results"]["unt"]["variance_percent"]}</td>                                 
                                </tr>
                              </tbody>
                            </table>"""
        email_message = email_message \
                        + self.style + table_html
        email_message = email_message + \
                        f'<br/><br/>'\
                        f'Here is a <a href="https://{self.RECON_DOMAIN}/{self.DASHBOARD}?Metric={fact_typ}&Batch+Date={curr_date}"" > report </a> if you would like more details'
        email_footer = f'<br/><br/>' \
                       f'*** Please do not reply to this message because it will end up in deep ' \
                       f'abyss where it will never be read. This is an automated message sent from an unmonitored email ' \
                       f'account that is used for outgoing communication only. ***' \
                       f'<br/><br/>' \
                       f'For support or data requests, please visit ' \
                       f'<a target="_blank" href="https://robling.io/askrobling/">https://robling.io/askrobling/</a> ' \
                       f'or send a message to ' \
                       f'<a href="mailto:askrobling@robling.io">askrobling@robling.io</a>.' \
                       f'<br/><br/><br/>' \
                       f'Best Regards,<br/><strong><label style="color:#00BCD4">' \
                       f'Team Robling</label></strong>' \
                       f'<br/><a target="_blank" href="https://robling.io">www.robling.io</a>'

        email_message = email_message + email_footer
        SendGridEmailModule.send_email(email_subject, email_message,
                                      self.RECON_EMAIL_LIST.split(','))

    def send_success_email(self, json_result):
        ### This function sends success email to recon recepients if validation passes
        curr_date = json_result["current_day"]
        fact_typ = json_result["fact_type"]
        checkpoint = json_result["checkpoint"]
        src_sys1 = checkpoint[json_result["source_systems"]["system1"]]
        src_sys2 = checkpoint[json_result["source_systems"]["system2"]]
        
        email_subject = f'Success! :: {self.EXEC_ENV}  {fact_typ} has passed reconciliation checkpoint on {curr_date}.'
        email_message = f'Hi all,'\
                        f'<br/>'\
                        f'<br/>'\
                        f'Robling ran a successful reconciliation checkpoint for {fact_typ} between {src_sys1} and {src_sys2}. '\
                        f'<br/><br/>Here are the numbers.'

        table_html = f"""<table border="1" class="dataframe">
                              <thead>
                                <tr style="text-align: right;">
                                  <th>Batch Date</th>
                                  <th>Checkpoint</th>
                                  <th>Total Retail</th>
                                  <th>Total Cost</th>
                                  <th>Total Units</th>
                                  <th>Diff. Retail</th>
                                  <th>Diff. Cost</th>
                                  <th>Diff. Units</th>
                                  <th>Var. % Retail</th>
                                  <th>Var. % Cost</th>
                                  <th>Var. % Units</th>
                                </tr>
                              </thead>
                              <tbody>
                                <tr>
                                  <td>{curr_date}</td>
                                  <td>{src_sys1}</td>
                                  <td>{json_result["results"]["rtl"]["system1_value"]}</td>
                                  <td>{json_result["results"]["cst"]["system1_value"]}</td>
                                  <td>{json_result["results"]["unt"]["system1_value"]}</td>
                                  <td></td>
                                  <td></td>
                                  <td></td>
                                  <td></td>
                                  <td></td>
                                  <td></td>                                 
                                </tr>
                                <tr>
                                  <td>{curr_date}</td>
                                  <td>{src_sys2}</td>
                                  <td>{json_result["results"]["rtl"]["system2_value"]}</td>
                                  <td>{json_result["results"]["cst"]["system2_value"]}</td>
                                  <td>{json_result["results"]["unt"]["system2_value"]}</td>
                                  <td>{json_result["results"]["rtl"]["difference"]}</td>
                                  <td>{json_result["results"]["cst"]["difference"]}</td>
                                  <td>{json_result["results"]["unt"]["difference"]}</td>
                                  <td>{json_result["results"]["rtl"]["variance_percent"]}</td>
                                  <td>{json_result["results"]["cst"]["variance_percent"]}</td>
                                  <td>{json_result["results"]["unt"]["variance_percent"]}</td>                                 
                                </tr>
                              </tbody>
                            </table>"""
        email_message = email_message \
                        + self.style + table_html
        email_message = email_message + \
                        f'<br/><br/>'\
                        f'Here is a <a href="https://{self.RECON_DOMAIN}/{self.DASHBOARD}?Metric={fact_typ}&Batch+Date={curr_date}"" > report </a> if you would like more details'
        email_footer = f'<br/><br/>' \
                       f'*** Please do not reply to this message because it will end up in deep ' \
                       f'abyss where it will never be read. This is an automated message sent from an unmonitored email ' \
                       f'account that is used for outgoing communication only. ***' \
                       f'<br/><br/>' \
                       f'For support or data requests, please visit ' \
                       f'<a target="_blank" href="https://robling.io/askrobling/">https://robling.io/askrobling/</a> ' \
                       f'or send a message to ' \
                       f'<a href="mailto:askrobling@robling.io">askrobling@robling.io</a>.' \
                       f'<br/><br/><br/>' \
                       f'Best Regards,<br/><strong><label style="color:#00BCD4">' \
                       f'Team Robling</label></strong>' \
                       f'<br/><a target="_blank" href="https://robling.io">www.robling.io</a>'

        email_message = email_message + email_footer
        SendGridEmailModule.send_email(email_subject, email_message,
                                      self.RECON_EMAIL_LIST.split(','))
        
    def send_reconciliation_email(self, json_result, query_result):
      data_variance_result = query_result.filter(regex='VAR_')
      no_data_result = query_result.filter(regex='.*(_RTL|_UNT|_CST)')
      validation_range_lower = json_result["validation_range"]["lower"]
      validation_range_upper = json_result["validation_range"]["upper"]
      status = json_result.get("status")
      warning_code = json_result.get("warning_code")

      if status == "warning" and warning_code == "RECON_NO_DATA":
          self.send_no_data_warning_email(json_result)
          return

      if (data_variance_result > 0).any().any() or (data_variance_result < 0).any().any():
          if (data_variance_result < int(validation_range_lower)).any().any() or (data_variance_result > int(validation_range_upper)).any().any():
              print("failure")
              self.send_failure_email(json_result)
              raise RuntimeError(f"Load Failed due to Reconciliation failure in : {json_result['fact_type']}")
          else:
              print("warning")
              self.send_warning_email(json_result)
      else:
          if (no_data_result > 0).any().any() or (no_data_result < 0).any().any():
              self.send_success_email(json_result)
          else:
              self.send_no_data_email(json_result)
              raise Exception(f"Load Failed due to no data available for reconciliation in : {json_result['fact_type']}")
