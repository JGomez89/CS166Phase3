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
import java.io.FileWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.*;

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

		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
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

	public static int findFirstMissing(int array[], int start, int end) {
		if (start > end)
			return end + 1;
		if (start != array[start])
			return start;
		int mid = (start + end) / 2;
		if (array[mid] == mid)
			return findFirstMissing(array, mid+1, end);
		return findFirstMissing(array, start, mid);
	}

	public static void ioWrite(String filename, String[] type, String[] datatype) {
		int n=type.length;
		String input[] = new String[n];
		for(int i=0; i<n; i++){
			do { // get input info with given prompts
				System.out.print("Please enter the " + type[i] + ": ");
				try {
					input[i] = in.readLine();
					break;
				}catch (Exception e) {
					System.out.println("Your input is invalid! " + datatype[i]);
					continue;
				}
			}while (true);
		}
	  try { // write the info into the given filename
	    FileWriter myWriter = new FileWriter("../../data/"+filename);
			BufferedReader br = new BufferedReader(new FileReader("../../data/"+filename));

			// find the smallest ID available in the file
			String sCurrentLine="";
			String idString="";
			int idInt=0;
			List<Integer> v = new ArrayList<Integer>();
			List<String[]> table = readData(filename);

	    for(int i=0; i<table.size(); i++){
					v.add(Integer.parseInt(table.get(i)[0]));
	    }
			Collections.sort(v);
			int[] array = v.stream().mapToInt(i->i).toArray();
			String availableID = String.valueOf( findFirstMissing(array,0,v.size()-1) );
			String output = availableID;

			for(int i=0; i<n; i++){
				output += ',' + input[i];
			}

	    myWriter.write(output+"\n");
	    myWriter.close();
	  } catch (Exception e) {
	    e.printStackTrace();
	  }
	}

	public static List<String[]> readData(String file) throws Exception {
    List<String[]> content = new ArrayList<>();
    try(BufferedReader br = new BufferedReader(new FileReader("../../data/"+file))) {
        String line = "";
        while ((line = br.readLine()) != null) {
            content.add(line.split(","));
        }
    } catch (Exception e) {
      //Some error logging
    }
		/*
			0,94,769,W
			1,245,18,R
			2,161,197,W
			3,104,1289,R
			returns:
				{	["0","94","769","W"],
					["1","245","18","R"],
					["2","161","197","W"],
					["3","104","1289","R"]}
		*/
    return content;
	}

	public static void AddShip(DBproject esql) {//1
		String filename = "Ships.csv";
		String type[] = new String[4];
		String datatype[] = new String[4];
		type[0] = "make";
		type[1] = "model";
		type[2] = "age";
		type[3] = "number of seats";
		datatype[0] = "(\"make\"=CHAR(20))";
		datatype[1] = "(\"model\"=CHAR(64))";
		datatype[2] = "(\"age\"=(int4>=0)";
		datatype[3] = "(\"seats\"=(500>int4>0)";
		ioWrite(filename,type,datatype);
	}

	public static void AddCaptain(DBproject esql) {//2
		String filename = "Captains.csv";
		String type[] = new String[2];
		String datatype[] = new String[2];
		type[0] = "fullname";
		type[1] = "nationality";
		datatype[0] = "(\"fullname\"=CHAR(128))";
		datatype[1] = "(\"nationality\"=CHAR(24))";
		ioWrite(filename,type,datatype);
	}

	public static void AddCruise(DBproject esql) {//3
		String filename = "Cruises.csv";
		String type[] = new String[7];
		String datatype[] = new String[7];
		type[0] = "cost";
		type[1] = "number of tickets sold";
		type[2] = "number of stops";
		type[3] = "actual departure date";
		type[4] = "actual arrival date";
		type[5] = "arrival port";
		type[6] = "departure port";
		datatype[0] = "(\"cost\"=_PINTEGER)";
		datatype[1] = "(\"num_sold\"=_PZEROINTEGER)";
		datatype[2] = "(\"num_stops\"=_PZEROINTEGER)";
		datatype[3] = "(\"actual_departure_date\"=DATE)";
		datatype[4] = "(\"actual_arrival_date\"=DATE)";
		datatype[5] = "(\"arrival_port\"=CHAR(5))";
		datatype[6] = "(\"departure_port\"=CHAR(5))";
		ioWrite(filename,type,datatype);
	}


	public static void BookCruise(DBproject esql) {//4
		// Given a customer and a Cruise that he/she wants to book, add a reservation to the DB
		String filename = "reservation.csv";
		String type[] = new String[3];
		String datatype[] = new String[3];
		type[0] = "customer's ID";
		type[1] = "cruise's ID";
		type[2] = "status";
		datatype[0] = "(\"ccid\"=INTEGER)";
		datatype[1] = "(\"cid\"=INTEGER)";
		datatype[2] = "(\"status\"=CHAR(1) IN ('W','C','R'))";
		ioWrite(filename,type,datatype);
	}

	public static void ListNumberOfAvailableSeats(DBproject esql) {//5
		// For Cruise number and date, find the number of availalbe seats (i.e. total Ship capacity minus booked seats )
	}

	public static void ListsTotalNumberOfRepairsPerShip(DBproject esql) {//6
		// Count number of repairs per Ships and list them in descending order
	}


	public static void FindPassengersCountWithStatus(DBproject esql) {//7
		// Find how many passengers there are with a status (i.e. W,C,R) and list that number.
	}
}
