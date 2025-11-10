package com.example.network_tool.db;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Bootstraps DuckDB:
 * 1) Connects to file-backed DB if app.dbPath is provided; else in-memory
 * 2) Loads CSVs into raw_* materialized tables (with safe
 * casting/normalization)
 * 3) Creates views with the original names so existing queries keep working
 */
@Component
public class DuckDb implements AutoCloseable {
    private final Connection conn;
    private final String dataDir;

    public DuckDb(
            @Value("${app.dataDir}") String dataDir,
            @Value("${app.dbPath:}") String dbPath) throws SQLException {
        this.dataDir = dataDir;

        File dd = new File(dataDir);
        if (!dd.exists() || !dd.isDirectory()) {
            throw new SQLException("app.dataDir does not exist or is not a directory: " + dataDir);
        }

        // If dbPath provided, make sure parent folder exists; else open in-memory
        String jdbcUrl;
        if (dbPath != null && !dbPath.isBlank()) {
            Path p = Paths.get(dbPath);
            File parent = p.toAbsolutePath().getParent() != null ? p.toAbsolutePath().getParent().toFile() : null;
            if (parent != null && !parent.exists() && !parent.mkdirs()) {
                throw new SQLException("Failed to create directories for app.dbPath: " + parent);
            }
            jdbcUrl = "jdbc:duckdb:" + p.toString();
        } else {
            jdbcUrl = "jdbc:duckdb:"; // in-memory
        }

        this.conn = DriverManager.getConnection(jdbcUrl);

        // ✅ Enable DuckDB Spatial (install only once per DB; load every start)
        try (Statement st = conn.createStatement()) {
            st.execute("INSTALL spatial");
            st.execute("LOAD spatial");

            st.execute("PRAGMA threads=" + Math.max(2, Runtime.getRuntime().availableProcessors()));

            st.execute("PRAGMA temp_directory='./duckdb_tmp'");
            st.execute("PRAGMA memory_limit='4GB'");
            st.execute("PRAGMA enable_progress_bar=false");

        }

        bootstrap(); // create raw_* materialized tables
        createViews(); // map views to original names
    }

    // Normalize separators so DuckDB sees forward slashes on Windows
    private String csv(String filename) {
        return Paths.get(dataDir, filename).toString().replace("\\", "/");
    }

    private void exec(Statement st, String sql, Object... args) throws SQLException {
        if (args != null && args.length > 0) {
            StringBuilder sb = new StringBuilder(sql);
            for (Object arg : args) {
                int idx = sb.indexOf("%s");
                if (idx < 0)
                    break;
                // Use single-quoted substitution for file paths / literals
                sb.replace(idx, idx + 2, arg.toString());
            }
            st.execute(sb.toString());
        } else {
            st.execute(sql);
        }
    }

    private void bootstrap() throws SQLException {
        try (Statement st = conn.createStatement()) {
            // People (DMV)
            exec(st, """
                        CREATE SCHEMA IF NOT EXISTS raw;
                        CREATE OR REPLACE TABLE raw_people AS
                        SELECT
                          TRIM(ssn)                                  AS ssn,
                          UPPER(TRIM(firstname))                     AS firstname,
                          UPPER(TRIM(middlename))                    AS middlename,
                          UPPER(TRIM(lastname))                      AS lastname,
                          UPPER(TRIM(driverslicense))                AS dl,
                          UPPER(TRIM(address))                       AS address_line1,
                          CAST(NULL AS VARCHAR)                      AS city,
                          CAST(NULL AS VARCHAR)                      AS state,
                          CAST(NULL AS VARCHAR)                      AS zip
                        FROM read_csv_auto('%s', header=true);
                    """, csv("DepartmentMotorVehicles.csv"));

            // Phone contracts (HSS / HLR)
            exec(st, """
                        CREATE OR REPLACE TABLE raw_phone_contracts AS
                        SELECT
                          TRIM(CAST(ssn AS VARCHAR))                                                             AS ssn,
                          REGEXP_REPLACE(TRIM(CAST(msisdn AS VARCHAR)), '[^0-9]', '', 'g')                       AS phone_norm,
                          TRIM(CAST(msisdn AS VARCHAR))                                                          AS phone,
                          UPPER(TRIM(CAST(make  AS VARCHAR)))                                                    AS device_make,
                          UPPER(TRIM(CAST(model AS VARCHAR)))                                                    AS device_model,
                          CAST(imsi AS VARCHAR)                                                                  AS imsi,
                          CAST(mcc  AS VARCHAR)                                                                  AS mcc,
                          CAST(mnc  AS VARCHAR)                                                                  AS mnc,
                          UPPER(TRIM(CAST(mac AS VARCHAR)))                                                      AS mac,
                          CAST(TRY_STRPTIME(CAST(contract_start_date AS VARCHAR), '%Y-%m-%d') AS DATE)           AS contract_start_date,
                          TRY_CAST(contract_term AS INTEGER)                                                     AS contract_term_months,
                          'CONTRACT'                                                                             AS phone_type
                        FROM read_csv_auto('%s', header=true);
                    """,
                    csv("HomeLocationRegistry.csv"));

            // Vehicles
            exec(st, """
                        CREATE OR REPLACE TABLE raw_vehicles AS
                        SELECT
                          UPPER(TRIM(CAST("Owner Drivers License" AS VARCHAR)))                                        AS owner_dl,
                          UPPER(TRIM(CAST("Owner First Name"     AS VARCHAR)))                                        AS owner_first,
                          UPPER(TRIM(CAST("Owner Middle Name"    AS VARCHAR)))                                        AS owner_middle,
                          UPPER(TRIM(CAST("Owner Last Name"      AS VARCHAR)))                                        AS owner_last,
                          UPPER(TRIM(CAST("Vehicle Make"         AS VARCHAR)))                                        AS make,
                          UPPER(TRIM(CAST("Vehicle Model"        AS VARCHAR)))                                        AS model,
                          TRY_CAST("Vehicle Year" AS INTEGER)                                                         AS year,
                          UPPER(TRIM(CAST("Vehicle Color"        AS VARCHAR)))                                        AS color,
                          UPPER(TRIM(CAST("State Registered"     AS VARCHAR)))                                        AS state_registered,
                          UPPER(TRIM(REGEXP_REPLACE(CAST("License Plate" AS VARCHAR), '[^A-Z0-9]', '', 'g')))        AS plate_norm,
                          UPPER(TRIM(CAST("License Plate"        AS VARCHAR)))                                        AS plate_raw,
                          UPPER(TRIM(CAST(VIN                    AS VARCHAR)))                                        AS vin_norm
                        FROM read_csv_auto('%s', header=true);
                    """,
                    csv("VehicleRegistration.csv"));

            // Tax / Employers
            exec(st, """
                        CREATE OR REPLACE TABLE raw_tax_employers AS
                        SELECT
                          TRIM(CAST("SSN" AS VARCHAR))                  AS ssn,
                          UPPER(TRIM(CAST("Employer" AS VARCHAR)))      AS employer_name,
                          UPPER(TRIM(CAST("Employer Address" AS VARCHAR))) AS employer_address,
                          CAST("Employer EIN" AS VARCHAR)               AS employer_ein,
                          UPPER(TRIM(CAST("Address" AS VARCHAR)))       AS filer_address,
                          UPPER(TRIM(CAST("First Name" AS VARCHAR)))    AS filer_first,
                          UPPER(TRIM(CAST("Middle Name" AS VARCHAR)))   AS filer_middle,
                          UPPER(TRIM(CAST("Last Name" AS VARCHAR)))     AS filer_last,
                          TRY_CAST("Part Time"     AS BOOLEAN)          AS part_time,
                          TRY_CAST("Filed"         AS BOOLEAN)          AS filed,
                          TRY_CAST("Paid"          AS BOOLEAN)          AS paid,
                          TRY_CAST("Garnishment"   AS BOOLEAN)          AS garnishment,
                          TRY_CAST("Investigation" AS BOOLEAN)          AS investigation
                        FROM read_csv_auto('%s', header=true);
                    """, csv("TaxData.csv"));

            // Ankle monitor
            exec(st, """
                        CREATE OR REPLACE TABLE raw_ankle AS
                        SELECT
                          TRIM(ssn)                                         AS person_ssn,
                          CAST(TRY_STRPTIME(CAST(datetime AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)  AS ts,
                          TRY_CAST(location_y AS DOUBLE)                    AS lat,
                          TRY_CAST(location_x AS DOUBLE)                    AS lon
                        FROM read_csv_auto('%s', header=true);
                    """, csv("JaredCombs_Ankle_Monitor.csv"));

            // Wi-Fi events
            exec(st, """
                        CREATE OR REPLACE TABLE raw_wifi_events_raw AS
                        SELECT
                          CAST(TRY_STRPTIME(CAST(datetime AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)  AS ts,
                          UPPER(TRIM(CAST(mac AS VARCHAR)))              AS device_mac,
                          UPPER(TRIM(CAST(sensor_id AS VARCHAR)))        AS sensor_id,
                          TRY_CAST(sensor_latitude  AS DOUBLE)           AS s_lat,
                          TRY_CAST(sensor_longitude AS DOUBLE)           AS s_lon,
                          ssid_1, ssid_2, ssid_3, ssid_4, ssid_5,
                          ssid_6, ssid_7, ssid_8, ssid_9, ssid_10
                        FROM read_csv_auto('%s', header=true);
                    """, csv("WIFI_2016-11-11.csv"));

            // Businesses
            exec(st, """
                        CREATE OR REPLACE TABLE raw_businesses AS
                        SELECT
                          MD5(
                            COALESCE(UPPER(TRIM(CAST(name AS VARCHAR))),'') || '|' ||
                            COALESCE(CAST(lat AS VARCHAR),'') || '|' ||
                            COALESCE(CAST(lon AS VARCHAR),'')
                          )                                                     AS biz_id,
                          UPPER(TRIM(CAST(name        AS VARCHAR)))             AS name,
                          UPPER(TRIM(CAST(address     AS VARCHAR)))             AS line1,
                          TRY_CAST(lat AS DOUBLE)                               AS b_lat,
                          TRY_CAST(lon AS DOUBLE)                               AS b_lon,
                          UPPER(TRIM(CAST(naics       AS VARCHAR)))             AS naics,
                          UPPER(TRIM(CAST(owner_firs  AS VARCHAR)))             AS owner_firs,
                          UPPER(TRIM(CAST(owner_midd  AS VARCHAR)))             AS owner_midd,
                          UPPER(TRIM(CAST(owner_last  AS VARCHAR)))             AS owner_last
                        FROM read_csv_auto('%s', header=true);
                    """, csv("Baltimore_Businesses.csv"));

            // LPR
            exec(st, """
                        CREATE OR REPLACE TABLE raw_lpr AS
                        SELECT
                          CAST(TRY_STRPTIME(CAST(datetime AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS TIMESTAMP)  AS ts,
                          UPPER(TRIM(REGEXP_REPLACE(CAST(licenseplate AS VARCHAR), '[^A-Z0-9]', '', 'g'))) AS plate_norm,
                          UPPER(TRIM(CAST(licenseplate AS VARCHAR)))                  AS plate_raw,
                          TRY_CAST(lpr_latitude  AS DOUBLE)                           AS lat,
                          TRY_CAST(lpr_longitude AS DOUBLE)                           AS lon,
                          UPPER(TRIM(CAST(state AS VARCHAR)))                         AS plate_state,
                          UPPER(TRIM(CAST(lpr_direction AS VARCHAR)))                 AS direction,
                          CAST(lpr_id AS VARCHAR)                                     AS sensor_id
                        FROM read_csv_auto('%s', header=true);
                    """,
                    csv("LPR_2016-11-11.csv"));

            // Crime reports
            exec(st, """
                        CREATE OR REPLACE TABLE raw_crime_reports AS
                        SELECT
                          TRIM(CAST(report_id   AS VARCHAR))  AS report_id,
                          TRY_CAST(lat AS DOUBLE)             AS c_lat,
                          TRY_CAST(lon AS DOUBLE)             AS c_lon,
                          TRIM(CAST("pre-text"  AS VARCHAR))  AS pre_text,
                          TRIM(CAST("post-text" AS VARCHAR))  AS post_text,
                          TRIM(CAST(file_path   AS VARCHAR))  AS file_path,
                          CAST(NULL AS TIMESTAMP)             AS ts
                        FROM read_csv_auto('%s', header=true);
                    """, csv("crime_reports.csv"));
        }
    }

    private void createViews() throws SQLException {
        try (Statement st = conn.createStatement()) {
            exec(st, "CREATE OR REPLACE VIEW people            AS SELECT * FROM raw_people;");
            exec(st, "CREATE OR REPLACE VIEW phone_contracts   AS SELECT * FROM raw_phone_contracts;");
            exec(st, "CREATE OR REPLACE VIEW vehicles          AS SELECT * FROM raw_vehicles;");
            exec(st, "CREATE OR REPLACE VIEW tax_employers     AS SELECT * FROM raw_tax_employers;");
            exec(st, "CREATE OR REPLACE VIEW ankle             AS SELECT * FROM raw_ankle;");
            exec(st, "CREATE OR REPLACE VIEW wifi_events_raw   AS SELECT * FROM raw_wifi_events_raw;");
            exec(st, "CREATE OR REPLACE VIEW businesses        AS SELECT * FROM raw_businesses;");
            exec(st, "CREATE OR REPLACE VIEW lpr               AS SELECT * FROM raw_lpr;");
            exec(st, "CREATE OR REPLACE VIEW crime_reports     AS SELECT * FROM raw_crime_reports;");
        }
    }

    public Connection conn() {
        return conn;
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }
}
