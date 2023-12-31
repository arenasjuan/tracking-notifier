import csv
import datetime
import json
import traceback
import mysql.connector
import numpy as np
import requests
from dateutil import tz
from psycopg2 import extras, connect, Error, ProgrammingError, OperationalError
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import config


# Get Pacific timezone object
tz_us_pacific = tz.gettz('US/Pacific')

now = datetime.datetime.now(tz_us_pacific)

# Determine whether it's morning or afternoon
if now.hour < 12:
    time_of_day = "Morning"
else:
    time_of_day = "Afternoon"

def lambda_handler(event, context):
    cnx = connect(
        dbname=config.dbname, 
        user=config.user, 
        password=config.pw, 
        host=config.host, 
        port=config.port
    )
    cursor = cnx.cursor(cursor_factory=extras.DictCursor)

    # SendGrid setup
    sg = SendGridAPIClient(config.ALCHEMIST_SENDGRID_API_KEY)

    # Select data from database
    query = "SELECT * FROM shipments"
    cursor.execute(query)
    problem_order_data = {}
    delay_order_data = {}
    stuck_order_data = {}
    alert_order_data = {}
    count_query = "SELECT COUNT(*) FROM shipments;"
    cursor.execute(count_query)
    total_shipments = cursor.fetchone()[0]
    cursor.execute(query)
    processed_shipments = 0
    problem_orders = 0
    errors = 0
    error_orders = []
    delivered = 0
    alerts = 0


    def calculate_days(date1, date2):
        if isinstance(date1, str):
            date1 = datetime.datetime.strptime(date1, "%Y%m%d").date()

        start = date1
        end = date2.date()
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

    def get_usps_access_token():
        oauth_url = "https://api.usps.com/oauth2/v3/token"
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'client_id': config.USPS_CLIENT_ID,
            'client_secret': config.USPS_CLIENT_SECRET,
            'grant_type': 'client_credentials',
            'customer_registration_id': config.USPS_CUSTOMER_REGISTRATION_ID,
            "mailer_id": config.USPS_MAILER_ID,
        }

        response = requests.post(oauth_url, headers=headers, json=data)  # Using json=data

        if response.status_code == 200:
            response_json = response.json()
            return response_json['access_token']
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None

    def move_row(cursor, order_number, source_table, target_table, notification_update=False):
        # Optional notification update
        if notification_update:
            try:
                notification_update_query = f"UPDATE {source_table} SET \"NotificationSent\"='Yes' WHERE \"OrderNumber\"=%s;"
                cursor.execute(notification_update_query, (order_number,))
            except Error as e:
                print(f"Error updating notification status for order {order_number}: {e}")

        # Copy row from source_table to target_table
        try:
            move_query = f"INSERT INTO {target_table} SELECT * FROM {source_table} WHERE \"OrderNumber\"=%s;"
            cursor.execute(move_query, (order_number,))
        except Error as e:
            print(f"Error moving order {order_number} to {target_table}: {e}")

        # Delete row from source_table
        try:
            delete_query = f"DELETE FROM {source_table} WHERE \"OrderNumber\"=%s;"
            cursor.execute(delete_query, (order_number,))
        except Error as e:
            print(f"Error deleting order {order_number} from {source_table}: {e}")

    
    def column_update(cursor, order_number, update_values):
        try:
            set_clause = ", ".join([f"\"{column}\"=%s" for column in update_values.keys()])
            update_query = f"UPDATE shipments SET {set_clause} WHERE \"OrderNumber\"=%s;"
            cursor.execute(update_query, (*update_values.values(), order_number))
        except (Error, ProgrammingError, OperationalError) as e:
            print(f"Database error occurred while updating columns: {e}")

    def fetch_column_value(cursor, order_number, *columns):
        column_names = ", ".join([f"\"{column}\"" for column in columns])
        try:
            fetch_query = f"SELECT {column_names} FROM shipments WHERE \"OrderNumber\"=%s;"
            cursor.execute(fetch_query, (order_number,))
            result = cursor.fetchone()
            if result is not None:
                if len(result) == 1:
                    return result[0]
                else:
                    return result
        except (Error, ProgrammingError, OperationalError) as e:
            print(f"Database error occurred while fetching column values: {e}")
        return None

    def add_to_email(order_data_dict, status_entry, row):
        # Structure of the data to be added
        data_to_add = {
            'order_number': row['OrderNumber'],
            'customer_name': row['CustomerName'],
            'customer_email': row['CustomerEmail'],
            'tracking_number': row['TrackingNumber'],
            'shipped_date': row['ShippedDate']
        }
        # Add data to the appropriate order data dictionary
        order_data_dict.setdefault(status_entry, []).append(data_to_add)

    def generate_order_rows(code, orders, bg_color, stuck=False):
        if 'Pre' not in code:
            if stuck:
                code = code[5:]
            elif code[:3] == '003':
                code = "Status Stuck at 'Shipment Ready for UPS'"

        order_rows = f"""
            <tr style="background-color: {bg_color};">
                <td rowspan="{len(orders)}" style="width: 20%; border: 1px solid black; padding: 5px;"><b>{code}</b></td>
                <td style="border: 1px solid black; padding: 5px;">{orders[0]['order_number']}</td>
                <td style="border: 1px solid black; padding: 5px;">{orders[0]['tracking_number']}</td>
                <td style="border: 1px solid black; padding: 5px;">{orders[0]['shipped_date']}</td>
                <td style="border: 1px solid black; padding: 5px;">{orders[0]['customer_name']}</td>
                <td style="border: 1px solid black; padding: 5px;">{orders[0]['customer_email']}</td>
            </tr>
        """
        for order in orders[1:]:
            order_rows += f"""
                <tr style="background-color: {bg_color};">
                    <td style="border: 1px solid black; padding: 5px;">{order['order_number']}</td>
                    <td style="border: 1px solid black; padding: 5px;">{order['tracking_number']}</td>
                    <td style="border: 1px solid black; padding: 5px;">{order['shipped_date']}</td>
                    <td style="border: 1px solid black; padding: 5px;">{order['customer_name']}</td>
                    <td style="border: 1px solid black; padding: 5px;">{order['customer_email']}</td>
                </tr>
            """
        return order_rows

    if 'body' in event:
        event_body = json.loads(event['body'])

        if "database_entries" in event_body:
            print("Received database entries list")
            database_entries = event_body['database_entries']

            if not database_entries:
                print(f"Database entries list empty; ending execution")
            else:
                num_new_entries = len(database_entries)

                print(f"Adding {num_new_entries} shipments from new shipment batch to database")
                print(f"Full batch for reference: {database_entries}")

                counter = 0
                for entry in database_entries:
                    counter += 1
                    print(f"Processing order {counter} of {num_new_entries}")

                    sql = '''
                        INSERT INTO "shipments" (
                            "OrderNumber", "CustomerName", "CustomerEmail", "TrackingNumber", "CarrierName", "ShippedDate", "StatusCode", "LastLocation", 
                            "DaysAtLastLocation", "NotificationSent", "Delayed", "Delivered",
                            "STK", "A1GG", "A1LLF", "GreenGlow", "IronStrength", "LiquidLush", "MightyMicros", 
                            "SoilBalance", "TurfTonic", "MosquitoDefense", "LawnGuard", "LoneStarLawnFood", 
                            "WeedWizardBottle", "Sprayer", "LawnGuardSprayer", "WandSprayer"
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT ("OrderNumber") DO UPDATE 
                        SET 
                            "CustomerName" = EXCLUDED."CustomerName",
                            "CustomerEmail" = EXCLUDED."CustomerEmail",
                            "TrackingNumber" = EXCLUDED."TrackingNumber",
                            "CarrierName" = EXCLUDED."CarrierName",
                            "ShippedDate" = EXCLUDED."ShippedDate",
                            "StatusCode" = EXCLUDED."StatusCode",
                            "LastLocation" = EXCLUDED."LastLocation",
                            "DaysAtLastLocation" = EXCLUDED."DaysAtLastLocation",
                            "NotificationSent" = EXCLUDED."NotificationSent",
                            "Delayed" = EXCLUDED."Delayed",
                            "Delivered" = EXCLUDED."Delivered",
                            "STK" = EXCLUDED."STK",
                            "A1GG" = EXCLUDED."A1GG",
                            "A1LLF" = EXCLUDED."A1LLF",
                            "GreenGlow" = EXCLUDED."GreenGlow",
                            "IronStrength" = EXCLUDED."IronStrength",
                            "LiquidLush" = EXCLUDED."LiquidLush",
                            "MightyMicros" = EXCLUDED."MightyMicros",
                            "SoilBalance" = EXCLUDED."SoilBalance",
                            "TurfTonic" = EXCLUDED."TurfTonic",
                            "MosquitoDefense" = EXCLUDED."MosquitoDefense",
                            "LawnGuard" = EXCLUDED."LawnGuard",
                            "LoneStarLawnFood" = EXCLUDED."LoneStarLawnFood",
                            "WeedWizardBottle" = EXCLUDED."WeedWizardBottle",
                            "Sprayer" = EXCLUDED."Sprayer",
                            "LawnGuardSprayer" = EXCLUDED."LawnGuardSprayer",
                            "WandSprayer" = EXCLUDED."WandSprayer"
                        WHERE "shipments"."TrackingNumber" != EXCLUDED."TrackingNumber";
                    '''

                    data = (
                        entry['OrderNumber'], entry['CustomerName'], entry['CustomerEmail'], entry['TrackingNumber'], entry['CarrierName'], entry['ShippedDate'],
                        entry['StatusCode'], entry['LastLocation'], entry['DaysAtLastLocation'], entry['NotificationSent'], entry['Delayed'], entry['Delivered'],
                        entry.get('STK', 0), entry.get('A1GG', 0), entry.get('A1LLF', 0), entry.get('GreenGlow', 0), entry.get('IronStrength', 0), entry.get('LiquidLush', 0),
                        entry.get('MightyMicros', 0), entry.get('SoilBalance', 0), entry.get('TurfTonic', 0), entry.get('MosquitoDefense', 0), entry.get('LawnGuard', 0),
                        entry.get('LoneStarLawnFood', 0), entry.get('WeedWizardBottle', 0), entry.get('Sprayer', 0), entry.get('LawnGuardSprayer', 0), entry.get('WandSprayer', 0)
                    )

                    try:
                        cursor.execute(sql, data)
                    except Error as e:
                        print(f"Error inserting or updating order #{entry['OrderNumber']} into shipments table: {e}")
                        cnx.commit()  # Commit the successful transactions before the rollback
                        cnx.rollback()  # Rollback the transaction
                        continue
                            
                cnx.commit()

        return

    current_date = datetime.datetime.now(tz_us_pacific)

    usps_auth_token = get_usps_access_token()

    ups_auth_token = get_ups_token()

    for row in cursor.fetchall():
        processed_shipments += 1
        print(f"Processing shipment {processed_shipments} out of {total_shipments}")
        if row["ShippedDate"] == current_date.date():
            continue
        try:
            tracking_number = row['TrackingNumber']
            carrier_name = row['CarrierName']
            try:
                if carrier_name == 'USPS':
                    # Handle USPS tracking
                    url = f'https://api.usps.com/tracking/v3/tracking/{tracking_number}?expand=DETAIL'
                    headers = {
                        'Authorization': 'Bearer ' + usps_auth_token
                    }
                    response = requests.get(url, headers=headers)
                    details = response.json()

                    # current_location = details['trackingEvents'][0]['eventCity']

                    # if row["LastLocation"] == current_location:
                    #     days_at_last_location = calculate_days(row["ShippedDate"], current_date)

                    #     if days_at_last_location >= 3 and :
                    #         problem_orders += 1
                    #         move_row(cursor, row['OrderNumber'], "shipments", "problem_orders", True)
                    #         add_to_email(problem_order_data, "Status Stuck at 'Pre-Shipment' for 3 or More Business Days", row)
                        
                    #     # Update DaysAtLastLocation with the calculated days
                    #     column_update(cursor, row['OrderNumber'], {"DaysAtLastLocation": days_since_shipped})

                    if details.get('trackingEvents'):
                        if isinstance(details['trackingEvents'], list):
                            last_location = details['trackingEvents'][0]['eventCity']
                        elif isinstance(details['trackingEvents'], dict):
                            last_location = details['trackingEvents'].get('eventCity', 'Unknown')
                        else:
                            print(f"Unexpected type or value for trackingEvents: {type(details.get('trackingEvents'))}, {details.get('trackingEvents')}")
                            continue

                        column_update(cursor, row['OrderNumber'], {"StatusCode": details['statusCategory'], "LastLocation": last_location})
                    else:
                        print(f"TrackingEvents key not found in details.")
                        continue


                    if 'Delivered' in details['statusCategory']:
                        delivered += 1
                        column_update(cursor, row['OrderNumber'], {"Delivered": 'Yes'})
                        move_row(cursor, row['OrderNumber'], "shipments", "delivered")
                    elif details['statusCategory'] == 'Pre-Shipment' and details['statusCategory'] == row["StatusCode"]:
                        # Calculate days since the ShippedDate
                        days_since_shipped = calculate_days(row["ShippedDate"], current_date)

                        if days_since_shipped >= 3:
                            problem_orders += 1
                            move_row(cursor, row['OrderNumber'], "shipments", "problem_orders", True)
                            add_to_email(stuck_order_data, "Status Stuck at 'Pre-Shipment' for 3 or More Business Days", row)
                        
                        # Update DaysAtLastLocation with the calculated days
                        column_update(cursor, row['OrderNumber'], {"DaysAtLastLocation": days_since_shipped})

                    elif details['statusCategory'] == 'Alert':
                        if details['status'] in config.problem_codes_usps:
                            problem_orders += 1
                            move_row(cursor, row['OrderNumber'], "shipments", "problem_orders", True)
                            add_to_email(problem_order_data, details['status'], row)
                        else:
                            if row['NotificationSent'] == 'No':
                                alerts += 1
                                column_update(cursor, row['OrderNumber'], {"NotificationSent": 'Yes'})
                                add_to_email(alert_order_data, details['status'], row)
                    continue

                elif carrier_name == 'UPS':
                    response = requests.get(
                        "https://onlinetools.ups.com/api/track/v1/details/" + tracking_number,
                        headers={
                            "Content-Type": "application/json",
                            "transId": config.trans_id,
                            "transactionSrc": config.transaction_src,
                            "Authorization": "Bearer " + ups_auth_token,
                        },
                        params={
                            "locale": "en_US",
                            "returnSignature": "false"
                        }
                    )
                else:
                    continue

                details = response.json()

            except json.JSONDecodeError:
                print(f"Failed to get valid JSON response for tracking number: {tracking_number}")

            if 'trackResponse' not in details:
                print(f"'trackResponse' missing in details for {tracking_number}.")
                continue

            if 'shipment' not in details['trackResponse']:
                print(f"'shipment' missing in 'trackResponse' for {tracking_number}.")
                continue

            if not details['trackResponse']['shipment']:
                print(f"'shipment' in 'trackResponse' is empty for {tracking_number}.")
                continue

            if 'package' not in details['trackResponse']['shipment'][0]:
                print(f"'package' missing in 'shipment' for {tracking_number}.")
                continue

            if not details['trackResponse']['shipment'][0]['package']:
                print(f"'package' in 'shipment' is empty for {tracking_number}.")
                continue

            if 'activity' not in details['trackResponse']['shipment'][0]['package'][0]:
                print(f"'activity' missing in 'package' for {tracking_number}.")
                continue

            package_details = details['trackResponse']['shipment'][0]['package'][0]
            activity = package_details['activity'][0]
            status_code = package_details['currentStatus']['code']
            status_code_desc = package_details['currentStatus']['description']
            new_status_entry = f"{status_code}: {status_code_desc}"
            is_delivered = status_code in config.delivered_codes
            is_problem_code = status_code in config.problem_codes_ups
            is_delayed = status_code in config.delay_codes

            # Update the StatusCode in the database
            column_update(cursor, row['OrderNumber'], {"StatusCode": new_status_entry})

            # Check for '003' status code and days since shipment
            if status_code == '003' and calculate_days(row['ShippedDate'], current_date) >= 3:
                is_problem_code = True

            # Process current location
            try:
                current_location = activity['location']['address']['city']
                result = fetch_column_value(cursor, row['OrderNumber'], "LastLocation", "LastLocationDate")
                if result is not None:
                    previous_location, previous_location_date = result
                    # Check if LastLocation exists
                    if previous_location:
                        if current_location != previous_location:
                            # Update LastLocation, set LastLocationDate to activity date and DaysAtLastLocation to the difference between activity date and current date
                            days_at_location = calculate_days(activity['date'], current_date)
                            column_update(cursor, row['OrderNumber'], {"LastLocation": current_location, "LastLocationDate": activity['date'], "DaysAtLastLocation": days_at_location})
                        else:
                            # Calculate days at current location
                            if previous_location_date is not None:
                                days_at_location = calculate_days(previous_location_date, current_date)
                                column_update(cursor, row['OrderNumber'], {"DaysAtLastLocation": days_at_location})

                                # If shipment hasn't moved, only address it if it has no estimated delivery date
                                if days_at_location >= 3 and not package_details['deliveryDate']:
                                    notification_status = fetch_column_value(cursor, row['OrderNumber'], "NotificationSent")
                                    if days_at_location >= 5 and notification_status == 'No':
                                        problem_orders += 1
                                        add_to_email(stuck_order_data, '999: 5 Business Days without a Location Update, and No Delivery Date Found', row)
                                        move_row(cursor, row['OrderNumber'], "shipments", "problem_orders", True)

                                    elif days_at_location == 3:
                                        if notification_status == 'No':
                                            problem_orders += 1
                                            add_to_email(stuck_order_data, '998: 3 Business Days without a Location Update, and No Delivery Date Found', row)
                                            column_update(cursor, row['OrderNumber'], {"NotificationSent": 'Yes'})
                                        else:
                                            column_update(cursor, row['OrderNumber'], {"NotificationSent": 'No'})

                            else:
                                # If LastLocationDate is None, set it to the current date and DaysAtLastLocation to 0
                                column_update(cursor, row['OrderNumber'], {"LastLocationDate": current_date, "DaysAtLastLocation": 0})

                else:
                    print(f"No record found for order: {row['OrderNumber']}")
                    continue
            except Exception as e:
                print(f"Error processing order {row['OrderNumber']}: {e}")
            if is_delivered:
                delivered += 1
                column_update(cursor, row['OrderNumber'], {"Delivered": 'Yes'})
                move_row(cursor, row['OrderNumber'], "shipments", "delivered")

            elif is_problem_code:
                if is_delayed:
                    # Fetch the current state of the Delayed column
                    delayed_status = fetch_column_value(cursor, row['OrderNumber'], "Delayed")
                    # If the order is already marked as delayed, skip this iteration
                    if delayed_status == 'No':
                        problem_orders += 1
                        add_to_email(delay_order_data, new_status_entry, row)
                        column_update(cursor, row['OrderNumber'], {"Delayed": 'Yes'})
                else:
                    problem_orders += 1
                    add_to_email(problem_order_data, new_status_entry, row)
                    move_row(cursor, row['OrderNumber'], "shipments", "problem_orders", True)


        except Exception as e:
            errors += 1
            error_orders.append((row['OrderNumber'], row['CustomerName'], row['TrackingNumber']))
            print(f"Exception caught by master try-except block for order {row['OrderNumber']}: {e}\n{traceback.format_exc()}")
            continue

    report_content = f"""\
    <br><u><b>Processing Counts</b></u><br>
    # of Orders Processed: {total_shipments}<br>
    # of Orders Delivered: {delivered}<br>
    # of Problem Orders: {problem_orders}<br>
    # of Orders with Tracking Errors: {errors}<br>
    # of Alerts: {alerts}<br><br>
    <table style="width: 100%; border: 1px solid black;">
        <tr><th colspan="6" style="font-size:18px;">Problem Orders</th></tr>
        <tr>
            <th style="border: 1px solid black; padding: 5px;">Problem Code</th>
            <th style="border: 1px solid black; padding: 5px;">Order Number</th>
            <th style="border: 1px solid black; padding: 5px;">Tracking Number</th>
            <th style="border: 1px solid black; padding: 5px;">Ship Date</th>
            <th style="border: 1px solid black; padding: 5px;">Customer Name</th>
            <th style="border: 1px solid black; padding: 5px;">Customer Email</th>
        </tr>
    """

    # Handle stuck orders first
    for code, orders in stuck_order_data.items():
        if code[4] == ':':  # Check if the 4th character is a colon
            color_key = code[:3]  # Extract the three numbers
        else:
            color_key = code  # Use the entire order code

        bg_color = config.email_color_codes.get(color_key, "#ffffff")
        report_content += generate_order_rows(code, orders, bg_color, True)

    # Handle non-delayed problem orders
    for code, orders in problem_order_data.items():
        if code[4] == ':':  # Check if the 4th character is a colon
            color_key = code[:3]  # Extract the three numbers
        else:
            color_key = code  # Use the entire order code

        bg_color = config.email_color_codes.get(color_key, "#ffffff")
        report_content += generate_order_rows(code, orders, bg_color)


    # Handle delayed problem orders
    for code, orders in delay_order_data.items():
        color_key = code[:3]
        bg_color = config.email_color_codes.get(color_key, "#ffffff")
        report_content += generate_order_rows(code, orders, bg_color)

    report_content += "</table>"

    if alerts > 0:
        report_content += f"""
        <br><br><table style="width: 100%; border: 1px solid black;">
            <tr><th colspan="6" style="font-size:18px;">Alert Orders</th></tr>
            <tr>
                <th style="border: 1px solid black; padding: 5px;">Alert Code</th>
                <th style="border: 1px solid black; padding: 5px;">Order Number</th>
                <th style="border: 1px solid black; padding: 5px;">Tracking Number</th>
                <th style="border: 1px solid black; padding: 5px;">Ship Date</th>
                <th style="border: 1px solid black; padding: 5px;">Customer Name</th>
                <th style="border: 1px solid black; padding: 5px;">Customer Email</th>
            </tr>
        """

        # Handle alert orders
        for code, orders in alert_order_data.items():
            color_key = code
            bg_color = config.email_color_codes.get(color_key, "#ffffff")
            report_content += generate_order_rows(code, orders, bg_color)

        report_content += "</table>"



    if error_orders:
        report_content += "<br><br><u><b>Received error message when trying to track the following order(s):</b></u><br><ul>"
        for order_number, customer, tracking in error_orders:
            report_content += f"<li>#{order_number} {customer}: {tracking}</li>"
        report_content += "</ul>"

    subject_line = f"[TRACKING REPORT] {now.strftime('%m-%d-%Y')} {time_of_day}"
    report_email = Mail(
        from_email= config.from_email,
        to_emails= config.to_emails,
        subject=subject_line,
        html_content=report_content
    )

    print(f"Stuck shipments: {stuck_order_data}")
    print(f"Problem shipments: {problem_order_data}")
    print(f"Delay shipments: {delay_order_data}")
    print(f"Alert shipments: {alert_order_data}")

    try:
        response = sg.send(report_email)
        print("Execution report email sent successfully.")
    except Exception as e:
        print(f"Error sending execution report email: {e}")
    cnx.commit()
    cnx.close()