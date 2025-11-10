# Network Tool

This is a small Spring Boot console application that uses DuckDB to explore data stored in CSV files. It can show information about people, their phones, vehicles, and businesses, and reveal how these records connect.

---

## Requirements

- **IntelliJ IDEA** (Community or Ultimate edition)
- **JDK 17**
- **Git** to clone the project

---

## Data

The CSV files needed to run the app are included in the `data/` folder at the project root:


⚠️ One file, `Telco_2016-11-11.csv`, was too large to include in GitHub (it’s over 100 MB).  Everything else is there.

---

## How to run it in IntelliJ

1. **Clone the repository**
    - Open IntelliJ IDEA.
    - On the welcome screen, choose **Get from VCS**.
    - Paste the repository URL and click **Clone**.

2. **Open the project**
    - IntelliJ will import it automatically as a Maven project.
    - Wait for indexing and dependency download to finish.

3. **Set the JDK**
    - Go to *File → Project Structure → Project*.
    - Make sure the Project SDK is set to **17**.

4. **Run the application**
    - Open `src/main/java/com/example/network_tool/Application.java`.
    - Right-click the `main` method → **Run 'Application.main()'**.
    - If needed, set the **Working Directory** in the run configuration to the project root so `./data` resolves correctly.

5. **Using the tool**
    - Once the app starts, it will prompt you to enter an **SSN**.
    - Type in the SSN of the person you want information on.
    - The tool will pull matching records from the CSVs in the `data/` folder and display connections, businesses, vehicles, and other related information.

That’s it — the data is already included, so you can start right away.