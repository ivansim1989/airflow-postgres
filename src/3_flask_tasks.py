from flask import Flask, jsonify, request
import pandas as pd
import csv

app = Flask(__name__)
app.config['DEBUG'] = True


@app.route('/employee', methods=['GET'])
def get_employees():
    """
    Read employee data from the CSV file
    """
    # Read employee data from the CSV file
    employees = []
    with open('employees.csv', 'r') as file:
        reader = csv.DictReader(file)
        employees = list(reader)
    
    return jsonify(employees), 200

@app.route('/employee/<int:id>', methods=['GET'])
def get_employee(id):
    """
    Read employee data from the CSV file and find the employee with the given id
    Args:
        id: Given id
    """
    employee = None
    with open('employees.csv', 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if int(row['id']) == id:
                employee = row
                break
    
    if employee:
        return jsonify(employee), 200
    else:
        return 'Employee not found', 404

@app.route('/employee', methods=['POST'])
def add_employee():
    """
    Read the new employee data from the request body and add it to the CSV file
    """
    new_employee = request.get_json()
    with open('employees.csv', 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=new_employee.keys())
        writer.writerow(new_employee)
    
    return 'Employee added', 201

@app.route('/employee/<int:id>', methods=['POST'])
def update_employee(id):
    """
    Read the updated employee data from the request body and update the CSV file
    Args:
        id: Given id
    """
    updated_employee = request.get_json()
    with open('employees.csv', 'r') as file:
        reader = csv.DictReader(file)
        rows = list(reader)
    
    employee_found = False
    with open('employees.csv', 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            if int(row['id']) == id:
                writer.writerow(updated_employee)
                employee_found = True
            else:
                writer.writerow(row)
    
    if employee_found:
        return 'Employee updated', 200
    else:
        return 'Employee not found', 404

@app.route('/employee/<int:id>', methods=['DELETE'])
def delete_employee(id):
    """
    Delete the employee with the given id from the CSV file
    Args:
        id: Given id
    """
    rows = []
    with open('employees.csv', 'r') as file:
        reader = csv.DictReader(file)
        rows = list(reader)
    
    employee_found = False
    with open('employees.csv', 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            if int(row['id']) == id:
                employee_found = True
            else:
                writer.writerow(row)
    
    if employee_found:
        return 'Employee deleted', 200
    else:
        return 'Employee not found', 404

if __name__ == '__main__':
    host = '0.0.0.0'
    port = 5000 
    app.run(host=host, port=port)