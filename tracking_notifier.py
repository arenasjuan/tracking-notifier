import csv
import config
import datetime
import json
import numpy as np
import requests
import mysql.connector
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import datetime
from dateutil import tz
from psycopg2 import extras, connect, Error

# Get Pacific timezone object
tz_us_pacific = tz.gettz('US/Pacific')

now = datetime.datetime.now(tz_us_pacific)

# Determine whether it's morning or afternoon
if now.hour < 12:
    time_of_day = "Morning"
else:
    time_of_day = "Afternoon"

def lambda_handler(event, context):

    def calculate_days(date1, date2):
        date2 = datetime.datetime.strptime(date2, "%Y%m%d").date()
            
        start = date1
        end = date2
        step = datetime.timedelta(days=1)
        count = 0
        while start <= end:
            if start not in config.us_holidays_2023.keys() and start.weekday() < 5:
                count += 1
            start += step
        return count


    def get_ups_token():
        oauth_url = "https://onlinetools.ups.com/security/v1/oauth/token"
        payload = {
            "grant_type": "client_credentials"
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        response = requests.post(oauth_url, data=payload, headers=headers, auth=(config.UPS_CLIENT_ID, config.UPS_CLIENT_SECRET))
        if response.status_code != 200:
            print("Error occurred: ", response.text)  # Print the error message
            response.raise_for_status()  # This will raise an exception if the request failed
        data = response.json()
        return str(data["access_token"])

    cnx = connect(
        dbname=config.dbname, 
        user=config.user, 
        password=config.pw, 
        host=config.host, 
        port=config.port
    )
    cursor = cnx.cursor(cursor_factory=extras.DictCursor)

    if 'body' in event:
        # Load event body content
        event_body = json.loads(event['body'])

        # If we received database entries, add them to the database
        if "database_entries" in event_body:
            print("Received database entries list")
            database_entries = event_body['database_entries']

            if not database_entries:
                print(f"Database entries list empty; ending execution")
            else:
                num_new_entries = len(database_entries)

                print(f"Adding {num_new_entries} shipments from new shipment batch to database")
                print(f"Full batch for reference: {database_entries}")
                # Iterate over the entries

                counter = 0
                for entry in database_entries:
                    counter += 1
                    print(f"Processing order {counter} of {num_new_entries}")
                    # Prepare the SQL statement
                    sql = '''
                        INSERT INTO "shipments" ("OrderNumber", "CustomerName", "CustomerEmail", "TrackingNumber", "CarrierName", "ShippedDate", "StatusCode", "LastLocation", "DaysAtLastLocation", "NotificationSent", "Delayed", "Delivered")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    '''


                    # Create a tuple with all the values to insert
                    data = (entry['OrderNumber'], entry['CustomerName'], entry['CustomerEmail'], entry['TrackingNumber'], entry['CarrierName'], entry['ShippedDate'], entry['StatusCode'], entry['LastLocation'], entry['DaysAtLastLocation'], entry['NotificationSent'], entry['Delayed'], entry['Delivered'])

                    # Execute the SQL statement
                    cursor.execute(sql, data)
                
                # Commit the changes
                cnx.commit()

        return



    auth_token = get_ups_token()

    # SendGrid setup
    sg = SendGridAPIClient(config.SENDGRID_API_KEY)

    # Select data from database
    query = "SELECT * FROM shipments"
    cursor.execute(query)
    problem_order_data = {}
    count_query = "SELECT COUNT(*) FROM shipments;"
    cursor.execute(count_query)
    total_shipments = cursor.fetchone()[0]
    cursor.execute(query)
    processed_shipments = 0
    errors = 0
    error_orders = []
    delivered = 0


    for row in cursor.fetchall():
        processed_shipments += 1
        print(f"Processing shipment {processed_shipments} out of {total_shipments}")
        try:
            print(list(row.keys()))
            tracking_number = row['TrackingNumber']
            # Make a request to the UPS tracking API
            try:
                response = requests.get(
                    "https://onlinetools.ups.com/api/track/v1/details/" + tracking_number,
                    headers={
                        "Content-Type": "application/json",
                        "transId": config.trans_id,
                        "transactionSrc": config.transaction_src,
                        "Authorization": "Bearer " + auth_token,
                    },
                    params={
                        "locale": "en_US",
                        "returnSignature": "false"
                    }
                )
                details = response.json()
            except json.JSONDecodeError:
                print(f"Failed to get valid JSON response for tracking number: {tracking_number}")
                continue

            if ('trackResponse' not in details 
                or 'shipment' not in details['trackResponse'] 
                or not details['trackResponse']['shipment']  
                or 'package' not in details['trackResponse']['shipment'][0]  
                or not details['trackResponse']['shipment'][0]['package'] 
                or 'activity' not in details['trackResponse']['shipment'][0]['package'][0]):
                    print(f"Tracking details not found for {tracking_number}.")
                    continue

            package_details = details['trackResponse']['shipment'][0]['package'][0]
            activity = package_details['activity'][0]
            status_code = package_details['currentStatus']['code']
            status_code_desc = package_details['currentStatus']['description']
            new_status_entry = f"{status_code}: {status_code_desc}"
            is_delivered = status_code in config.delivered_codes
            is_problem_code = status_code in config.problem_codes_ups
            is_delayed = status_code in config.delay_codes
            is_stuck = False

            # Update the StatusCode in the database
            try:
                update_query = "UPDATE shipments SET \"StatusCode\"=%s WHERE \"OrderNumber\"=%s;"
                cursor.execute(update_query, (new_status_entry, row['OrderNumber']))
            except (Error, psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
                print(f"Database error occurred while updating status code: {e}")
                continue

            current_date = datetime.datetime.now(tz_us_pacific).strftime("%Y%m%d")
            # Check for '003' status code and days since shipment
            if status_code == '003' and calculate_days(row['ShippedDate'], current_date) > 2:
                is_problem_code = True

            # Process current location
            try:
                current_location = activity['location']['address']['city']
                # Fetch the previous location and the date it was updated
                try:
                    fetch_location_query = "SELECT \"LastLocation\", \"LastLocationDate\" FROM shipments WHERE \"OrderNumber\"=%s;"
                    cursor.execute(fetch_location_query, (row['OrderNumber'],))
                    result = cursor.fetchone()
                except (Error, psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
                    print(f"Database error occurred while fetching location: {e}")
                    continue
                # Check if a row was returned from the database
                if result is not None:
                    previous_location, previous_location_date = result
                    # Check if LastLocation exists
                    if previous_location:
                        if current_location != previous_location:
                            # Update LastLocation, set LastLocationDate to current date and DaysAtLastLocation to 0
                            try:
                                update_query = "UPDATE shipments SET \"LastLocation\"=%s, \"LastLocationDate\"=%s, \"DaysAtLastLocation\"=0 WHERE \"OrderNumber\"=%s;"
                                cursor.execute(update_query, (current_location, current_date, row['OrderNumber']))
                            except (Error, psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
                                print(f"Database error occurred while updating last location: {e}")
                                continue
                        else:
                            # Calculate days at current location
                            if previous_location_date is not None:
                                days_at_location = calculate_days(previous_location_date, current_date)
                                try:
                                    update_query = "UPDATE shipments SET \"DaysAtLastLocation\"=%s WHERE \"OrderNumber\"=%s;"
                                    cursor.execute(update_query, (days_at_location, row['OrderNumber']))
                                except (Error, psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
                                    print(f"Database error occurred while updating days at last location: {e}")
                                    continue
                                if days_at_location >= 3:
                                    is_problem_code = True
                                    is_stuck = True
                            else:
                                # If LastLocationDate is None, set it to the current date and DaysAtLastLocation to 0
                                try:
                                    update_query = "UPDATE shipments SET \"LastLocationDate\"=%s, \"DaysAtLastLocation\"=0 WHERE \"OrderNumber\"=%s;"
                                    cursor.execute(update_query, (current_date, row['OrderNumber']))
                                except (Error, psycopg2.ProgrammingError, psycopg2.OperationalError) as e:
                                    print(f"Database error occurred while updating last location date: {e}")
                                    continue
                else:
                    print(f"No record found for order: {row['OrderNumber']}")
                    continue
            except Exception as e:
                print(f"Error processing order {row['OrderNumber']}: {e}")
            if is_delivered:
                delivered += 1
                try:
                    delivered_query = "UPDATE shipments SET \"Delivered\"='Yes' WHERE \"OrderNumber\"=%s;"
                    cursor.execute(delivered_query, (row['OrderNumber'],))
                    # Move to delivered orders table
                    try:
                        delivered_orders_query = "INSERT INTO delivered SELECT * FROM shipments WHERE \"OrderNumber\"=%s;"
                        cursor.execute(delivered_orders_query, (row['OrderNumber'],))
                    except Error as e:
                        print(f"Error moving order {row['OrderNumber']} to delivered orders: {e}")
                    try:
                        delete_query = "DELETE FROM shipments WHERE \"OrderNumber\"=%s;"
                        cursor.execute(delete_query, (row['OrderNumber'],))
                    except Error as e:
                        print(f"Error deleting order {row['OrderNumber']} from shipments: {e}")
                except Error as e:
                    print(f"Error setting order {row['OrderNumber']} as delivered: {e}")
            elif is_problem_code:
                if is_delayed:
                    # Fetch the current state of the Delayed column
                    try:
                        fetch_delayed_query = "SELECT \"Delayed\" FROM shipments WHERE \"OrderNumber\"=%s;"
                        cursor.execute(fetch_delayed_query, (row['OrderNumber'],))
                        delayed_status = cursor.fetchone()
                    except Error as e:
                        print(f"Error fetching delayed status for order {row['OrderNumber']}: {e}")
                        continue
                    # If the order is already marked as delayed, skip this iteration
                    if delayed_status[0] == 'Yes':
                        continue
                # Compose email
                subject_issue = config.email_subject_issues[status_code] if not is_stuck else "NO MOVT FOR 3 DAYS"
                subject = f"[{subject_issue}] — Order #{row['OrderNumber']}"
                email = Mail(
                    from_email= config.from_email,
                    to_emails= config.to_emails,
                    subject=subject,
                    html_content=f"""\
                    <b><u>Customer name:</u></b><br>{row['CustomerName']}<br><br>
                    <b><u>Order number:</u></b><br>{row['OrderNumber']}<br><br>
                    <b><u>Ship date:</u></b><br>{row['ShippedDate']}<br><br>
                    <b><u>Customer email:</u></b><br>{row['CustomerEmail']}<br><br>
                    <b><u>Tracking number:</u></b><br>{row['TrackingNumber']}<br><br>
                    <b><u>Status:</u></b><br>{new_status_entry}<br><br>
                    """
                )
                try:
                    # Send email
                    response = sg.send(email)
                    print(f"Email for order #{row['OrderNumber']} sent successfully.")
                    # If the email send operation was successful, update the database accordingly
                    if is_delayed:
                        # Set Delayed to 'Yes' in the database
                        try:
                            delayed_query = "UPDATE shipments SET \"Delayed\"='Yes' WHERE \"OrderNumber\"=%s;"
                            cursor.execute(delayed_query, (row['OrderNumber'],))
                        except Error as e:
                            print(f"Error updating delayed status for order {row['OrderNumber']}: {e}")
                    else:
                        # Set NotificationSent to 'Yes' in the database
                        try:
                            notif_sent_query = "UPDATE shipments SET \"NotificationSent\"='Yes' WHERE \"OrderNumber\"=%s;"
                            cursor.execute(notif_sent_query, (row['OrderNumber'],))
                        except Error as e:
                            print(f"Error updating notification status for order {row['OrderNumber']}: {e}")
                        # Move to problem_orders table and remove from shipments table
                        try:
                            problem_orders_query = "INSERT INTO problem_orders SELECT * FROM shipments WHERE \"OrderNumber\"=%s;"
                            cursor.execute(problem_orders_query, (row['OrderNumber'],))
                        except Error as e:
                            print(f"Error moving order {row['OrderNumber']} to problem orders: {e}")
                        try:
                            delete_query = "DELETE FROM shipments WHERE \"OrderNumber\"=%s;"
                            cursor.execute(delete_query, (row['OrderNumber'],))
                        except Error as e:
                            print(f"Error deleting order {row['OrderNumber']} from shipments: {e}")
                    # Count the problem codes for all orders with successful email sending
                    problem_order_data.setdefault(new_status_entry, []).append(
                        {
                            'order_number': row['OrderNumber'],
                            'customer_name': row['CustomerName'],
                            'customer_email': row['CustomerEmail'],
                            'tracking_number': row['TrackingNumber']
                        }
                    )
                except Exception as e:
                    print(f"Error sending email for order {row['OrderNumber']}: {e}")


        except Exception as e:
            errors += 1
            error_orders.append((row['OrderNumber'], row['CustomerName'], row['TrackingNumber']))
            print(f"Exception caught by master try-except block: {e}")
            continue
    # Sending final progress report email.
    report_content = f"""\
    <u><b>Processing Counts</b></u><br><br>
    # of Orders Processed: {total_shipments}<br>
    # of Orders Delivered: {delivered}<br>
    # of Problem Orders: {sum(len(v) for v in problem_order_data.values())}<br>
    # of Orders with Tracking Errors: {errors}<br><br>
    <u><b><Problem Code Counts</b></u>
    """
    for code, orders in problem_order_data.items():
        report_content += f"<br><u>{code}</u> — <b>{len(orders)}</b>"
        report_content += "<ul>"
        for order in orders:
            report_content += f"<li>#{order['order_number']}: {order['customer_name']} ({order['customer_email']}) — {order['tracking_number']}</li>"
        report_content += "</ul>"

    if error_orders:
        report_content += "<br><br><u><b>Ran into tracking errors with the following order(s):</b></u><br><ul>"
        for order_number, customer, tracking in error_orders:
            report_content += f"<li>#{order_number} {customer}: {tracking}</li>"
        report_content += "</ul>"

    subject_line = f"[TRACKING EXECUTION REPORT] {now.strftime('%m-%d-%Y')} {time_of_day}"
    report_email = Mail(
        from_email= config.from_email,
        to_emails= config.to_emails,
        subject=subject_line,
        html_content=report_content
    )
    try:
        response = sg.send(report_email)
        print("Execution report email sent successfully.")
    except Exception as e:
        print(f"Error sending execution report email: {e}")
    cnx.commit()
    cnx.close()