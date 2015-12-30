package edu.ncsu.mas.platys.mturk.mpprivacystudy;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import com.opencsv.CSVReader;

public class MpPrivacyStudyMturkSemiAutoApprover {

  public static void main(String[] args) throws SQLException, FileNotFoundException, IOException {

    Properties props = new Properties();
    try (InputStream inStream = new FileInputStream("src/main/resources/application.properties")) {
      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));
    } catch (ClassNotFoundException | IllegalStateException | IOException e) {
      e.printStackTrace();
      return;
    }

    try (CSVReader reader = new CSVReader(
        new FileReader(props.getProperty("mturk.resultsFilename")));
        Connection conn = DriverManager.getConnection(props.getProperty("jdbc.url") + "?user="
            + props.getProperty("jdbc.username") + "&password="
            + props.getProperty("jdbc.password"));
        PreparedStatement preSurveyStmt = conn
            .prepareStatement("SELECT * from turker_presurvey_response "
                + "where LTRIM(RTRIM(mturk_id)) = ?");
        PreparedStatement postSurveyStmt = conn
            .prepareStatement("SELECT * from turker_postsurvey_response "
                + "where LTRIM(RTRIM(mturk_id)) = ?")) {

      reader.readNext(); // Ignore first line
      String[] line;
      while ((line = reader.readNext()) != null) {
        String workerId = line[15];
        String completionCode = line[27];

        System.out.println(workerId + "\n" + "================");

        postSurveyStmt.setString(1, workerId);
        try (ResultSet postsurveyRs = postSurveyStmt.executeQuery()) {
          while (postsurveyRs.next()) {
            if (postsurveyRs.getString("completion_code").trim().equals(completionCode)) {
              System.out.println("Completion Code: Matches");

              preSurveyStmt.setString(1, workerId);
              try (ResultSet presurveyRs = preSurveyStmt.executeQuery()) {
                while (presurveyRs.next()) {
                  System.out.println(presurveyRs.getString("gender"));
                }
              }
            } else {
              System.out.println("Completion Code: Does not match" + ", "
                  + postsurveyRs.getString("completion_code") + ", " + completionCode);
            }
          }
        }
        System.out.println();
      }
    }
  }
}
