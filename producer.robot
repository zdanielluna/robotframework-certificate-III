*** Settings ***
Documentation    Inhuman Insurance, Inc. Artificial Intelligence System robot.
...              Produce traffic data work items

Library          RPA.JSON
Library          RPA.Tables
Library          Collections
Resource         shared.robot

*** Variables ***
${URL}=                         https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json
${TRAFFIC_JSON_FILE_PATH}=      ${OUTPUT_DIR}${/}traffic.json
${COUNTRY_KEY}=                 SpatialDim
${GENDER_KEY}=                  Dim1
${RATE_KEY}=                    NumericValue
${YEAR_KEY}=                    TimeDim
${TEST_FILE}=                   test.csv

*** Tasks ***
Produce traffic data work items
    Download the traffic data
    ${traffic_data}=    Load traffic data as table
    ${filtered_data}=   Filter and sort traffic data    ${traffic_data}
    ${filtered_data}=   Get latest data by country      ${filtered_data}
    ${payloads}=        Create work item payloads       ${filtered_data}
    
    Save work item payloads    ${payloads}

*** Keywords ***
Download the traffic data
    Download
    ...    ${URL}
    ...    ${TRAFFIC_JSON_FILE_PATH}
    ...    overwrite=True

Load traffic data as table
    ${json}=              Load JSON from file    ${TRAFFIC_JSON_FILE_PATH}
    ${table}=             Create Table           ${json}[value]
    # Write table to CSV    ${table}               ${TEST_FILE}
    RETURN                ${table} 

Filter and sort traffic data
    [Arguments]         ${table}

    ${max_rate}=        Set Variable    ${5.0}
    ${both_genders}=    Set Variable    BTSX
    # ${rate_key}=        Set Variable    NumericValue
    # ${gender_key}=      Set Variable    Dim1
    # ${year_key}=        Set Variable    TimeDim

    Filter Table By Column    ${table}    ${RATE_KEY}      <       ${max_rate}
    Filter Table By Column    ${table}    ${GENDER_KEY}    ==      ${both_genders}
    Sort Table By Column      ${table}    ${YEAR_KEY}      False
    RETURN                    ${table}

Get latest data by country
    [Arguments]    ${table}

    ${table}=                     Group Table By Column    ${table}    ${COUNTRY_KEY}
    ${latest_data_by_country}=    Create List

    FOR    ${group}    IN    @{table}
        ${first_row}=    Pop Table Row                ${group}
        Append To List   ${latest_data_by_country}    ${first_row}
    END
    RETURN    ${latest_data_by_country}

Create work item payloads
    [Arguments]    ${traffic_data}
    ${payloads}=   Create List

    FOR    ${row}    IN    @{traffic_data}
        ${payload}=
        ...    Create Dictionary
        ...    country=${row}[${COUNTRY_KEY}]
        ...    year=${row}[${YEAR_KEY}]
        ...    rate=${row}[${RATE_KEY}]

        Append To List    ${payloads}    ${payload}
    END
    RETURN    ${payloads}

Save work item payloads
    [Arguments]    ${payloads}

    FOR    ${payload}    IN    @{payloads}
        Save work item payload    ${payload}
    END

Save work item payload
    [Arguments]    ${payload}

    ${variables}=              Create Dictionary        traffic_data=${payload}
    Create Output Work Item    variables=${variables}   save=True

