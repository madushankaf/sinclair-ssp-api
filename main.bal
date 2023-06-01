import ballerina/http;
import ballerina/sql;
import ballerinax/mssql;
import ballerina/uuid;
import ballerina/log;

configurable string dbHost = "";
configurable string dbUser = "";
configurable string dbPass = "";
configurable string dbName = "";
configurable int dbPort = 1433;

public type Person record {
    string id;
    string? first_name;
    string? last_name;
    string? primary_email_address;
    string? secondary_email_address;
    string? username;
    string? home_phone;
    string? cell_phone;
    string? work_phone;
    string? address_line_1;
    string? address_line_2;
    string? city;
    string? state;
    string? zip_code;
    string? school_id;
    string? birth_date;

};

public type ExternalPerson record {

    string? username;
    string? first_name;
    string? last_name;
    string? middle_name;
    string? primary_email_address;
    string? secondary_email_address;
    string? home_phone;
    string? cell_phone;
    string? work_phone;
    string? address_line_1;
    string? address_line_2;
    string? city;
    string? state;
    string? zip_code;
    string? school_id;
    string? birth_date;

};

final mssql:Client msSqlClient = check new (host = dbHost, user = dbUser, password = dbPass, database = dbName, port = dbPort);

service /sinclair_ssp on new http:Listener(9091) {
    resource function post persons(@http:Payload Person person) returns http:Ok|http:InternalServerError|error? {

        if person.id.length() == 0 {
            person.id = uuid:createType1AsString();
        }

        sql:ParameterizedQuery insertQuery = `
        INSERT INTO dbo.persons (id, first_name, last_name, 
        primary_email_address, secondary_email_address, username,
         home_phone, cell_phone, work_phone, address_line_1,
        address_line_2, city, state, zip_code, school_id, birth_date) VALUES ( +
        ${person.id},${person.first_name},${person.last_name},${person.primary_email_address},${person.secondary_email_address},
        ${person.username},${person.home_phone},${person.cell_phone},
        ${person.work_phone},${person.address_line_1},${person.address_line_2},
        ${person.city},${person.state},${person.zip_code},${person.school_id},${person.birth_date}
         )`;

        sql:ExecutionResult|sql:Error insertResult = msSqlClient->execute(insertQuery);
        if insertResult is sql:Error {
            log:printError("Error occurred while inserting data into the database", err = insertResult.message());
            return http:INTERNAL_SERVER_ERROR;
        }
        else {
            log:printInfo("Data inserted successfully: ", insertedId = person.id);
            return http:OK;
        }

    }

    resource function get persons(string? schooldId) returns http:Ok|http:NotFound|http:InternalServerError|error?|Person[] {

        sql:ParameterizedQuery selectQuery = ``;
        if schooldId != () && schooldId.length() == 0 {
            selectQuery = `SELECT * FROM person`;
        }
        else {
            selectQuery = `SELECT * FROM person WHERE school_id = ${schooldId}`;
        }
        stream<Person, sql:Error?> resultStream = msSqlClient->query(selectQuery);

        Person[]|error persons = from Person person in resultStream
            select person;

        if persons is sql:Error {
            log:printError("Error occurred while retrieving data from the database for schoolId", schooldId = schooldId, err = persons.message());
            return http:INTERNAL_SERVER_ERROR;
        }
        else if persons is error {
            return error("nternal error occurred while retrieving data from the database for schoolId", schooldId = schooldId, err = persons.message());
        }
        else if persons.length() == 0 {
            return http:NOT_FOUND;
        }
        else {
            return persons;
        }
    }

    resource function get persons/[string personGuid]() returns http:Ok|http:InternalServerError|http:NotFound|error?|Person {
        sql:ParameterizedQuery selectQuery = `SELECT * FROM person WHERE id = ${personGuid}`;
        stream<Person, sql:Error?> resultStream = msSqlClient->query(selectQuery);
        Person[]|error persons = from Person person in resultStream
            select person;

        if persons is sql:Error {
            log:printError("Error occurred while retrieving data from the database for schoolId", personGuid = personGuid, err = persons.message());
            return http:INTERNAL_SERVER_ERROR;
        }
        else if persons is error {
            return error("nternal error occurred while retrieving data from the database for personGuid", personGuid = personGuid, err = persons.message());
        }
        else if persons.length() > 1 {
            return error("Date integrity issues observerd in the database");
        }
        else if persons.length() == 0 {
            return http:NOT_FOUND;
        }
        else {
            return persons[0];
        }

    }

    resource function put persons/[string personGuid](@http:Payload Person person) returns http:Ok|http:NotFound|http:InternalServerError|error? {
        sql:ParameterizedQuery selectQuery = `SELECT * FROM person WHERE id = ${personGuid}`;
        stream<Person, sql:Error?> resultStream = msSqlClient->query(selectQuery);
        Person[]|error existingPersons = from Person _person in resultStream
            select _person;

        if existingPersons is sql:Error {
            log:printError("Error occurred while retrieving data from the database for schoolId", personGuid = personGuid, err = existingPersons.message());
            return http:INTERNAL_SERVER_ERROR;
        }
        else if existingPersons is error {
            return error("nternal error occurred while retrieving data from the database for personGuid", personGuid = personGuid, err = existingPersons.message());
        }
        else if existingPersons.length() > 1 {
            return error("Date integrity issues observerd in the database");
        }
        else if existingPersons.length() == 0 {
            return http:NOT_FOUND;
        }
        else {
            Person existingPerson = existingPersons[0];
            sql:ParameterizedQuery updateQuery = `UPDATE person SET 
            first_name = ${person.first_name.toString().length() == 0 ? existingPerson.first_name : person.first_name.toString()}, 
            last_name = ${person.last_name.toString().length() == 0 ? existingPerson.last_name : person.last_name.toString()},
            primary_email_address = ${person.primary_email_address.toString().length() == 0 ? existingPerson.primary_email_address : person.primary_email_address.toString()},
            secondary_email_address = ${person.secondary_email_address.toString().length() == 0 ? existingPerson.secondary_email_address : person.secondary_email_address.toString()},
            username = ${person.username.toString().length() == 0 ? existingPerson.username : person.username.toString()},
            home_phone = ${person.home_phone.toString().length() == 0 ? existingPerson.home_phone : person.home_phone.toString()},
            cell_phone = ${person.cell_phone.toString().length() == 0 ? existingPerson.cell_phone : person.cell_phone.toString()},
            work_phone = ${person.work_phone.toString().length() == 0 ? existingPerson.work_phone : person.work_phone.toString()},
            address_line_1 = ${person.address_line_1.toString().length() == 0 ? existingPerson.address_line_1 : person.address_line_1.toString()},
            address_line_2 = ${person.address_line_2.toString().length() == 0 ? existingPerson.address_line_2 : person.address_line_2.toString()},
            city = ${person.city.toString().length() == 0 ? existingPerson.city : person.city.toString()},
            state = ${person.state.toString().length() == 0 ? existingPerson.state : person.state.toString()},
            zip_code = ${person.zip_code.toString().length() == 0 ? existingPerson.zip_code : person.zip_code.toString()},
            school_id = ${person.school_id.toString().length() == 0 ? existingPerson.school_id : person.school_id.toString()},
            birth_date = ${person.birth_date.toString().length() == 0 ? existingPerson.birth_date : person.birth_date.toString()}
            WHERE id = ${personGuid}`;

            sql:ExecutionResult|sql:Error updateResult = msSqlClient->execute(updateQuery);
            if updateResult is sql:Error {
                log:printError("Error occurred while updating data into the database", err = updateResult.message());
                return http:INTERNAL_SERVER_ERROR;
            }
            else {
                log:printInfo("Data updated successfully: ", updatedId = person.id);
                return http:OK;
            }

        }

        // resource function post externalPersons(@http:Payload ExternalPerson externalPersons) returns http:Ok|http:InternalServerError|error? {

        //  }

        //  resource function get externalPersons(string schooldId, string username) returns http:Ok|http:InternalServerError|error? {
        //  }

        //  resource function put externalPersons/[string personGuid](@http:Payload ExternalPerson externalPersons) returns http:Ok|http:InternalServerError|error? {
        //  }
    }


}
