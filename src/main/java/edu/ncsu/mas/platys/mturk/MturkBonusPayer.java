package edu.ncsu.mas.platys.mturk;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;

public class MturkBonusPayer implements AutoCloseable {

  private final Properties props = new Properties();

  private final Connection mDbConn;

  private RequesterService mturkRequesterService;

  public MturkBonusPayer() throws FileNotFoundException, IOException, ClassNotFoundException,
      SQLException {
    try (InputStream inStream = new FileInputStream("src/main/resources/application.properties")) {
      props.load(inStream);
      Class.forName(props.getProperty("jdbc.driverClassName"));
      mDbConn = DriverManager.getConnection(props.getProperty("jdbc.url") + "?user="
          + props.getProperty("jdbc.username") + "&password=" + props.getProperty("jdbc.password"));
    }

    mturkRequesterService = new RequesterService(new PropertiesClientConfig("mturk.properties"));
  }

  private double getAccountBalance() {
    return mturkRequesterService.getAccountBalance();
  }

  private void grantBonus(String workerId, Double bonusAmount, String assignmentId,
      String bonusReason) {
    mturkRequesterService.grantBonus(workerId, bonusAmount, assignmentId, bonusReason);
  }

  private Map<String, Integer> getUserDetailsFromDb(int createdPhase) throws SQLException {
    Map<String, Integer> userIdToBonusAmount = new HashMap<>();

    String bonusUserQuery = "select mturk_id, bonus_amount from users_bonus"
        + " where bonus_type = 1 and created_phase = " + createdPhase;

    try (Statement stmt = mDbConn.createStatement();
        ResultSet rs = stmt.executeQuery(bonusUserQuery)) {
      while (rs.next()) {
        userIdToBonusAmount.put(rs.getString(1), rs.getInt(2));
      }
    }

    return userIdToBonusAmount;
  }

  // TODO: Not efficient to run an update for each payment
  private void updatePaymentInDb(String workerId) throws SQLException {
    String updatePaymentQuery = "update users_bonus set bonus_type = 2 where mturk_id = ?";
    try (PreparedStatement pStmt = mDbConn.prepareStatement(updatePaymentQuery)) {
      pStmt.setString(1, workerId);
      pStmt.executeUpdate();
    }
  }

  private static Integer sum(Collection<Integer> col) {
    Integer total = 0;
    for (Integer val : col) {
      total += val;
    }
    return total;
  }

  public static void main(String[] args) throws Exception {

    int createdPhase = Integer.parseInt(args[0]);
    String hitId = args[1];
    String bonusReason = args[2];

    try (MturkBonusPayer bonusPayer = new MturkBonusPayer()) {

      Map<String, Integer> userIdToBonusAmount = bonusPayer.getUserDetailsFromDb(createdPhase);
      System.out.println("Number of turkers to pay bonus: " + userIdToBonusAmount.size());

      Integer totalBonus = sum(userIdToBonusAmount.values());
      System.out.println("Total bonus to pay: " + totalBonus);

      Double balance = bonusPayer.getAccountBalance();
      System.out.println("Account balance: " + RequesterService.formatCurrency(balance));

      if (balance > (1.2 * totalBonus)) { // 20% Amazon fee
        for (String workerId : userIdToBonusAmount.keySet()) {
          bonusPayer.grantBonus(workerId, userIdToBonusAmount.get(workerId).doubleValue(), hitId,
              bonusReason);
          bonusPayer.updatePaymentInDb(workerId);
          System.out.println(workerId + ": " + userIdToBonusAmount.get(workerId));
        }
        System.out.println("Successfully paid all bonuses");
        System.out.println("Account balance: "
            + RequesterService.formatCurrency(bonusPayer.getAccountBalance()));
      } else {
        System.out.println("You do not have enough funds to pay the bonus.");
      }
    }
  }

  @Override
  public void close() throws Exception {
    mDbConn.close();
  }
}
