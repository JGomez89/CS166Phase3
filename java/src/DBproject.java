/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");

			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}

	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 *
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException {
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 *
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;

		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 *
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 * obtains the metadata object for the returned result set.  The metadata
		 * contains row and column info.
		*/
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;

		//iterates through the result set and saves the data returned by the query.
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>();
		while (rs.next()){
			List<String> record = new ArrayList<String>();
			for (int i=1; i<=numCol; ++i)
				record.add(rs.getString (i));
			result.add(record);
		}//end while
		stmt.close ();
		return result;
	}//end executeQueryAndReturnResult

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 *
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}

	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current
	 * value of sequence used for autogenerated keys
	 *
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */

	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();

		// select currval(pg_get_serial_sequence('names', 'id'));
		ResultSet rs = stmt.executeQuery ("Select currval(pg_get_serial_sequence("+sequence+"));\n");
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 *
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if

		DBproject esql = null;

		try{
			System.out.println("(1)");

			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}

			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];

			esql = new DBproject (dbname, dbport, user, "");

			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Ship");
				System.out.println("2. Add Captain");
				System.out.println("3. Add Cruise");
				System.out.println("4. Book Cruise");
				System.out.println("5. List number of available seats for a given Cruise.");
				System.out.println("6. List total number of repairs per Ship in descending order");
				System.out.println("7. Find total number of passengers with a given status");
				System.out.println("8. < EXIT");

				switch (readChoice()){
					case 1: AddShip(esql); break;
					case 2: AddCaptain(esql); break;
					case 3: AddCruise(esql); break;
					case 4: BookCruise(esql); break;
					case 5: ListNumberOfAvailableSeats(esql); break;
					case 6: ListsTotalNumberOfRepairsPerShip(esql); break;
					case 7: FindPassengersCountWithStatus(esql); break;
					case 8: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddShip(DBproject esql) {//1
		String key,make,model,age,seats; key = make = model = age = seats = "";

		System.out.print("Please enter the ship's make: ");
		try {
			make = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter the ship's model: ");
		try {
			model = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter the ship's age (YYYY): ");
		try {
			age = in.readLine();
			if (Integer.parseInt(age) < 0) {
				throw new Exception("xxxx");
			}
		}catch (Exception e) {
			if (Integer.parseInt(age) < 0) {
				System.out.println("Error! Age must be positive.");
			}
			else{System.out.println("Your input is invalid!");}
			return;
		}
		System.out.print("Please enter the number of seats on the ship: ");
		try {
			seats = in.readLine();
			if (Integer.parseInt(seats) <= 0 || Integer.parseInt(seats) >= 500) {
				throw new Exception("xxxx");
			}
		}catch (Exception e) {
			if (Integer.parseInt(seats) <= 0 || Integer.parseInt(seats) >= 500) {
				System.out.println("Error! Number of seats must be between 0 and 500.");
			}
			else{System.out.println("Your input is invalid!");}
			return;
		}


		try {
			key = String.valueOf(esql.getCurrSeqVal("'Ship','id'")+1);
		}catch (Exception SQLException){
			System.out.println("Error getting current squenve value!");
			return;
		}
		try {
			String csv_inputs = String.join(",",key,make,model,age,seats);
			String query = "INSERT INTO Ship VALUES ("+csv_inputs+");\n";
			esql.executeUpdate(query);
		}catch (Exception SQLException) {
			System.out.println("Error running query!");
			return;
		}
	}

	public static void AddCaptain(DBproject esql) {//2
		String key, fullname, nationality;

		System.out.print("Please enter captain's fullname: ");
		try {
			fullname = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter captain's nationality: ");
		try {
			nationality = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}

		try {
			key = String.valueOf(esql.getCurrSeqVal("'Captain','id'"));
			String csv_inputs = String.join(",", key, fullname, nationality);
			String query = "INSERT INTO Captain (id, fullname, nationality) VALUES ("+csv_inputs+");\n";
			esql.executeUpdate(query);
		}  catch (Exception SQLException)
		{ System.out.println("Error running query!");
			return;
		}
	}

	public static void AddCruise(DBproject esql) {//3
		String key, cost, num_sold, num_stops, actual_departure_date, actual_arrival_date, arrival_port, departure_port;

		System.out.print("Please enter cost of cruise: ");
		try {
			cost = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter number of tickets sold: ");
		try {
			num_sold = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter number of stops: ");
		try {
			num_stops = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter the actual departure date (YYYY-MM-DD): ");
		try {
			actual_departure_date = in.readLine();
			java.sql.Date.valueOf(actual_departure_date);
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter the actual arrival date (YYYY-MM-DD): ");
		try {
			actual_arrival_date = in.readLine();
			java.sql.Date.valueOf(actual_arrival_date);
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter the arrival_port: ");
		try {
			arrival_port = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter the departure port: ");
		try {
			departure_port = in.readLine();
		} catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}

		try {
			key = String.valueOf(esql.getCurrSeqVal("'Cruise','cnum'"));
			String csv_inputs = String.join(",", key, cost, num_sold, num_stops, actual_departure_date, actual_arrival_date, arrival_port, departure_port);
			String query = "INSERT INTO Cruise (cnum, cost, num_sold, num_stops, actual_departure_date, actual_arrival_date, arrival_port, departure_port) VALUES ("+csv_inputs+");\n";
			esql.executeUpdate(query);
		}  catch (Exception SQLException)
		{ System.out.println("Error running query!");
			return;
		}
	}


	public static void BookCruise(DBproject esql) {//4
		// Given a customer and a Cruise that he/she wants to book, add a reservation to the DB
		String key, ccid, cid, status;

		System.out.print("Please enter customer's ID: ");
		try {
			ccid = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter cruise's ID: ");
		try {
			cid = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		System.out.print("Please enter status: ");
		try {
			status = in.readLine();
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}

		try {
			key = String.valueOf(esql.getCurrSeqVal("'Reservation','rnum'"));
			String csv_inputs = String.join(",", key, ccid, cid, status);
			String query = "INSERT INTO Reservation (rnum, ccid, cid, status) VALUES ("+csv_inputs+");\n";
			esql.executeUpdate(query);
		}  catch (Exception SQLException)
		{ System.out.println("Error running query!");
			return;
		}
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//5
		// For Cruise number and date, find the number of availalbe seats (i.e. total Ship capacity minus booked seats )
		int crusNum;
		String date;
		String querySold;
		String queryShipID;
		String querySeats;
		int seats;
		int num_sold;
		int shipID;
		// returns only if a correct value is given.
		System.out.print("Please input cruise number: ");
		try { // read the integer, parse it and break.
			crusNum = Integer.parseInt(in.readLine());
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}//end try

		System.out.print("Please input departure date in the format YYYY-MM-DD: ");
		try {
			date = in.readLine();
			java.sql.Date.valueOf(date);
		} catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		// To find number of available seats subtract ship seats with cruise num_sold
		querySold = "SELECT COUNT (nuum_sold) FROM Cruise WHERE Cruise.cruz_num = " + crusNum + " AND Cruise.departure_date = " + date + ";\n";
		queryShipID = "SELECT ship_id FROM CruiseInfo WHERE cruise_id = " + crusNum + ";\n";
		try{
			shipID = Integer.parseInt(esql.executeQueryAndReturnResult(queryShipID).get(0).get(0));
		}  catch (Exception SQLException)
		{
			System.out.println("Error acquiring shipID.");
			return;
		}
		querySeats = "SELECT COUNT (seats) FROM Ship WHERE id = " + shipID + ";\n";
		try{
			num_sold = Integer.parseInt(esql.executeQueryAndReturnResult(querySold).get(0).get(0));
		}  catch (Exception SQLException)
		{
			System.out.println("Error acquiring tickets sold");
			return;
		}
		try {
			seats = Integer.parseInt(esql.executeQueryAndReturnResult(querySeats).get(0).get(0));
		}  catch (Exception SQLException)
		{
			System.out.println("Error acquiring seats on ship.");
			return;
		}

		System.out.print("There are: ");
		System.out.print(seats - num_sold);
		System.out.print(" seats available");
		return;

	}

	public static void ListsTotalNumberOfRepairsPerShip(DBproject esql) {//6
		// Count number of repairs per Ships and list them in descending order
		String query;
		query = "SELECT shipID FROM Repairs ORDER BY COUNT(ship_id) DESC;\n";
		try {
			esql.executeQueryAndPrintResult(query);
		}  catch (Exception SQLException)
		{
			System.out.println("Error acquiring repairs.");
			return;
		}


	}


	public static void FindPassengersCountWithStatus(DBproject esql) {//7
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
		// W means waitlisted, C means completed, and R means reserved
		int crusNum;
		String P_status;
		String query;

		// returns only if a correct value is given.
		System.out.print("Please input cruise number: ");
		try { // read the integer, parse it and break.
			crusNum = Integer.parseInt(in.readLine());
		}catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}//end try

		System.out.print("Please input W for waitlist, C for confirmed, or R for reservation: ");
		try {
			P_status = in.readLine();
		} catch (Exception e) {
			System.out.println("Your input is invalid!");
			return;
		}
		if(P_status != "W" && P_status != "C" && P_status != "R")
		{
			System.out.println("Your given status is invaluid!");
			return;
		}
		else // Print out passangers with
		{
			query = "SELECT COUNT (status) FROM Reservation WHERE cid = " + crusNum + " AND status = " + P_status + ";\n";
			try {
				esql.executeQueryAndPrintResult(query);
			} catch (Exception e)	{
				System.out.println("Error getting reservation.");
				return;
			}
		}
		return;
	}
}
