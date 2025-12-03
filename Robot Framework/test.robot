*** Settings ***
Library           SeleniumLibrary
Library           helper.py

Suite Setup       Open Report In Browser
Suite Teardown    Close Browser


*** Variables ***
${REPORT_FILE}           ${CURDIR}${/}report_from_container.html
${PARQUET_FOLDER}        ${CURDIR}${/}parquet_files${/}facility_type_avg_time_spent_per_visit_date
${PARQUET_DATE_COLUMN}   partition_date
${FILTER_DATE}           2025-11-20
${SVG_CELL_LOCATOR}      css=text.cell-text
${NUM_COLUMNS}           3


*** Test Cases ***
Compare HTML Report With Parquet Dataset
    [Documentation]    Compare the SVG-based table in the report against the Parquet snapshot.
    Wait Until Page Contains Element    ${SVG_CELL_LOCATOR}    10s
    ${cell_texts}=      Execute JavaScript    return Array.from(document.querySelectorAll('text.cell-text')).map(el => el.textContent.trim());
    ${report_df}=       Read Svg Table          ${cell_texts}    ${NUM_COLUMNS}
    ${parquet_df}=      Read Parquet Dataset    ${PARQUET_FOLDER}    ${FILTER_DATE}    ${PARQUET_DATE_COLUMN}
    ${differences}=     Compare Dataframes      ${report_df}    ${parquet_df}
    Should Be Equal As Strings    ${differences}    ${EMPTY}    msg=Data mismatch detected:\n${differences}


*** Keywords ***
Open Report In Browser
    ${report_uri}=    Path To Uri    ${REPORT_FILE}
    Open Browser    about:blank    Chrome
    Go To    ${report_uri}
    Maximize Browser Window

Close Browser
    Close All Browsers