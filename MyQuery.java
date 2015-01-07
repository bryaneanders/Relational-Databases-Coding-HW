/*****************************
*****************************/
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.Types;
import java.util.Date;
import java.lang.String;

public class MyQuery {

    private Connection conn = null;
	private Statement statement = null;
	private ResultSet resultSet = null;
	private BufferedWriter outfile;
    
    public MyQuery(Connection c)throws SQLException
    {
        conn = c;
        statement = conn.createStatement();
    }
    
    public void findBestOfficer() throws SQLException
    {
        String query = "SELECT first, last, cnt " +
					   "FROM (SELECT first, last, count(crime_id) as cnt " +
							 "FROM officers NATURAL JOIN crime_officers " +
							 "GROUP BY first, last) temp " +
					   "WHERE cnt >= ( SELECT avg(count)  " +
					   "FROM (SELECT count(crime_id) count " +
							 "FROM officers NATURAL JOIN crime_officers " +
							 "GROUP BY first, last) temp2);";
		
		resultSet = statement.executeQuery(query);
    }
    
    public void printBestOfficer() throws IOException, SQLException
    {
		outfile = new BufferedWriter(new FileWriter("result.txt"));
	
		System.out.println("******** Query 1 ********\n");
		outfile.write("******** Query 1 ********\n\n");
		
		System.out.println("first  |  last  |  cnt");
		System.out.println("-----------------------");
		outfile.write("first  |  last  |  cnt\n");
		outfile.write("-----------------------\n");
		
		String str;
		while (resultSet.next()) {
			str = String.format("%s | %s | %s\n",
								resultSet.getString("first"),
								resultSet.getString("last"),
								resultSet.getString("cnt"));
		
			System.out.print(str);
			outfile.write(str);
		}
    }

    public void findCrimeCharge() throws SQLException
    {
		String query = "SELECT charge_id " +
					   "FROM crime_charges " +
					   "WHERE fine_amount > (SELECT avg(fine_amount) " +
										    "FROM crime_charges) " +
					   "AND amount_paid < (SELECT avg(amount_paid) " +
										  "FROM crime_charges);";
												 
		resultSet = statement.executeQuery(query);
    }

    public void printCrimeCharge() throws IOException, SQLException
    {
		String str;
		
		System.out.println("\n\n******** Query 2 ********\n");
		System.out.println("charge_id");
		System.out.println("---------");
		outfile.write("\n\n******** Query 2 ********\n\n");
		outfile.write("charge_id\n");
		outfile.write("---------\n");
		
		while (resultSet.next()) {
		
			str = String.format("%s \n", resultSet.getString("charge_id"));
			
			System.out.print(str);
			outfile.write(str);
		}
    }

    public void findCriminal() throws SQLException
    {
		String query = "SELECT DISTINCT first, last " +
					   "FROM criminals NATURAL JOIN crimes " +
					   "NATURAL JOIN crime_charges " +
					   "WHERE crime_code IN (SELECT crime_code " +
										    "FROM crime_charges " +
										    "WHERE crime_id = 10085);";
 
		resultSet = statement.executeQuery(query);
    }

    public void printCriminal() throws IOException, SQLException
    {
		String str;
	
		System.out.println("\n\n******** Query 3 ********\n");
		System.out.println("first  |  last  ");
		System.out.println("----------------");
		outfile.write("\n\n******** Query 3 ********\n\n");
		outfile.write("first  |  last  \n");
		outfile.write("----------------\n");
		
		while (resultSet.next()) {
			str = String.format("%s | %s\n",
								resultSet.getString("first"),
								resultSet.getString("last"));
			
			System.out.print(str);
			outfile.write(str);			
		}
    }

    public void findCriminalSentence() throws SQLException
    {
		String query = "SELECT criminal_id, last, first, count(sentence_id) cnt_sentence " +
					   "FROM criminals NATURAL JOIN sentences " +
					   "GROUP BY criminal_id " +
					   "HAVING count(sentence_id) > 1;";
					   
		resultSet = statement.executeQuery(query);
    }

    public void printCriminalSentence() throws IOException, SQLException
    {
		String str;
		
		System.out.println("\n\n******** Query 4 ********\n");
		System.out.println("criminal_id  |  first  |  last  |  cnt_sentence");
		System.out.println("-----------------------------------------------");
		outfile.write("\n\n******** Query 4 ********\n\n");
		outfile.write("criminal_id  |  first  |  last  |  cnt_sentence\n");
		outfile.write("-----------------------------------------------\n");
		
		while (resultSet.next()) {
			str = String.format("%s    |   %s   |   %s   |   %s\n",
							    resultSet.getString("criminal_id"),
								resultSet.getString("first"),
								resultSet.getString("last"),
								resultSet.getString("cnt_sentence"));
			
			System.out.print(str);
			outfile.write(str);
		}
    }

    public void findChargeCount() throws SQLException
    {
		String query = "SELECT precinct, count(charge_id) cnt_charges " +
					   "FROM officers NATURAL JOIN crime_officers " +
					   "NATURAL JOIN crime_charges " +
					   "WHERE charge_status = 'GL' " +
					   "GROUP BY precinct " +
					   "HAVING count(charge_id) >= 7;";
					   
		resultSet = statement.executeQuery(query);			   
    }

    public void printChargeCount() throws IOException, SQLException
    {
		String str;
	
		System.out.println("\n\n******** Query 5 ********\n");
		System.out.println("precinct  |  cnt_charges");
		System.out.println("------------------------");
		outfile.write("\n\n******** Query 5 ********\n\n");
		outfile.write("precinct  |  cnt_charges\n");
		outfile.write("------------------------\n");
		
		while (resultSet.next()) {
		
			
			str = String.format("%s | %s\n",
										   resultSet.getString("precinct"),
										   resultSet.getString("cnt_charges"));
			
			System.out.print(str);
			outfile.write(str);
		}
    }
	
    public void findCrimeCounts() throws IOException, SQLException
    {
		/* My stored procedure:
		
			DELIMITER //
			CREATE PROCEDURE getNumber(
			IN off_id INT, 
			OUT num_arrests INT)
			BEGIN
			SELECT count(crime_id)
			INTO num_arrests
			FROM crime_officers
			WHERE officer_id = off_id;
			END //
			DELIMITER ;
		*/
	
		System.out.println("\n\n******** Query 6 ********\n");	
		outfile.write("\n\n******** Query 6 ********\n\n");	
		InputStreamReader istream = new InputStreamReader(System.in) ;
        BufferedReader bufRead = new BufferedReader(istream);
		
		int officer_id, cnt;
		
		try {
				System.out.println("Please enter the officer_id for the query: ");
				
				officer_id = Integer.parseInt(bufRead.readLine());
				//fill in this portion
				CallableStatement stmt = conn.prepareCall("{call getNumber(?, ?)}");
				
				stmt.setInt("off_id", officer_id);
				stmt.registerOutParameter("num_arrests", Types.INTEGER);
				
				boolean hasResults = stmt.execute();
				
				cnt = stmt.getInt("num_arrests");
			
				System.out.print("Officer " +officer_id+" has reported "+cnt+" crimes.\n");
				outfile.write("Officer " +officer_id+" has reported "+cnt+" crimes.\n");
		} catch (IOException err) {
				System.out.println("Error reading line");
		}
		
		outfile.close();
    }
}