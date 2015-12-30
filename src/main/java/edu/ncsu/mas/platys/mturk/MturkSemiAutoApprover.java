package edu.ncsu.mas.platys.mturk;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.opencsv.CSVReader;

public class MturkSemiAutoApprover {

  public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {

    Properties props = new Properties();
    try (InputStream inStream = new FileInputStream("src/main/resources/application.properties")) {
      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));
    } catch (ClassNotFoundException | IllegalStateException | IOException e) {
      e.printStackTrace();
      return;
    }

    Map<String, String> dbMturkIdToCompletionCodeMap = new HashMap<String, String>();

    try (CSVReader reader = new CSVReader(
        new FileReader(props.getProperty("mturk.resultsFilename")));
        Connection conn = DriverManager.getConnection(props.getProperty("jdbc.url") + "?user="
            + props.getProperty("jdbc.username") + "&password="
            + props.getProperty("jdbc.password"));
        Statement completionCodeStmt = conn.createStatement();
        ResultSet rs = completionCodeStmt.executeQuery(props.getProperty("completionCode.query"))) {

      while (rs.next()) {
        String dbMturkId = rs.getString("mturkId");
        String dbCompletionCode = rs.getString("completionCode");
        if (dbCompletionCode == null || dbCompletionCode.trim().length() == 0) {
          continue;
        }

        if (dbMturkIdToCompletionCodeMap.containsKey(dbMturkId)) {
          throw new IllegalStateException(dbMturkId + " has multiple completion codes");
        }

        dbMturkIdToCompletionCodeMap.put(dbMturkId.trim(), dbCompletionCode.trim());
      }

      reader.readNext(); // Ignore first line
      String[] line;
      while ((line = reader.readNext()) != null) {
        String onlineMturkId = line[15].trim();
        String onlineCompletionCode = line[27].trim();

        if (!onlineCompletionCode.equals(dbMturkIdToCompletionCodeMap.get(onlineMturkId))) {
          System.out.println("Completion code does not match for " + onlineMturkId + " (Online: "
              + onlineCompletionCode + "; DB: " + dbMturkIdToCompletionCodeMap.get(onlineMturkId));
        }
      }
    }
  }
}
