import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GradeAssignment1 {

	// Assuming the program has been compiled using Gradle.
	// Assuming the database URL contains an empty schema.
	public static void main(String[] args) throws Exception {
		String folderToAssignment = args[0];
		String dbURL = args[1];
		String user = args[2];
		String pwd = args[3];
		String pathToIMDBData = args[4];
		String maxMem = args[5];
		
		ProcessBuilder builder = new ProcessBuilder(folderToAssignment + "IMDBLoader", dbURL + "?rewriteBatchedStatements=true", user, pwd, pathToIMDBData).redirectErrorStream(true);
		builder.environment().put("IMDB_LOADER_OPTS", "-Xmx" + maxMem + "m");
		
		StringBuffer reasons = new StringBuffer();
		AtomicBoolean notCompleted = new AtomicBoolean(false), notInTime = new AtomicBoolean(false), schemaIssues = new AtomicBoolean(false), 
				dataIssues = new AtomicBoolean(false), weirdIssues = new AtomicBoolean(false);
		
		Process process = null;
		try {
			long before = System.nanoTime();
			process = builder.start();
			StreamGobbler gobbler = new StreamGobbler(process.getInputStream(), false);
			gobbler.start();
			boolean done = process.waitFor(3, TimeUnit.HOURS);
			long after = System.nanoTime();
			double timeTaken = (after - before) / (1e9 * 3600);
			notInTime.set(timeTaken > 2.0 && timeTaken < 3.0);
			
			reasons.append("The process took " + timeTaken + " hours; ");
			
			if (!done) {
				// Not on time!
				reasons.append("The process did not run in less than three hours; ");
				notCompleted.set(true);
			} else {
				Connection con = DriverManager.getConnection(dbURL, user, pwd);
				
				Map<String, Integer> columns = new HashMap<>();
				columns.put("personId".toLowerCase(), Types.INTEGER);
				columns.put("movieId".toLowerCase(), Types.INTEGER);
				checkTable(con, "ActedIn".toLowerCase(), columns, 3178736, reasons, schemaIssues, dataIssues);
				
				columns = new HashMap<>();
				columns.put("personId".toLowerCase(), Types.INTEGER);
				columns.put("movieId".toLowerCase(), Types.INTEGER);
				checkTable(con, "ComposedBy".toLowerCase(), columns, 491352, reasons, schemaIssues, dataIssues);
				
				columns = new HashMap<>();
				columns.put("personId".toLowerCase(), Types.INTEGER);
				columns.put("movieId".toLowerCase(), Types.INTEGER);
				checkTable(con, "DirectedBy".toLowerCase(), columns, 999858, reasons, schemaIssues, dataIssues);
				
				columns = new HashMap<>();
				columns.put("personId".toLowerCase(), Types.INTEGER);
				columns.put("movieId".toLowerCase(), Types.INTEGER);
				checkTable(con, "EditedBy".toLowerCase(), columns, 320875, reasons, schemaIssues, dataIssues);
				
				columns = new HashMap<>();
				columns.put("id".toLowerCase(), Types.INTEGER);
				columns.put("name".toLowerCase(), Types.VARCHAR);
				checkTable(con, "Genre".toLowerCase(), columns, 29, reasons, schemaIssues, dataIssues);
				
				columns = new HashMap<>();
				columns.put("movieId".toLowerCase(), Types.INTEGER);
				columns.put("genreId".toLowerCase(), Types.INTEGER);
				checkTable(con, "HasGenre".toLowerCase(), columns, 2431016, reasons, schemaIssues, dataIssues);
				
				columns = new HashMap<>();
				columns.put("id".toLowerCase(), Types.INTEGER);
				columns.put("title".toLowerCase(), Types.VARCHAR);
				columns.put("releaseYear".toLowerCase(), Types.INTEGER);
				columns.put("runtime".toLowerCase(), Types.INTEGER);
				columns.put("rating".toLowerCase(), Types.REAL);
				columns.put("numberOfVotes".toLowerCase(), Types.INTEGER);
				checkTable(con, "Movie".toLowerCase(), columns, 1379465, reasons, schemaIssues, dataIssues);
				
				columns = new HashMap<>();
				columns.put("id".toLowerCase(), Types.INTEGER);
				columns.put("name".toLowerCase(), Types.VARCHAR);
				checkTable(con, "Person".toLowerCase(), columns, 9706922, reasons, schemaIssues, dataIssues);
				
				columns = new HashMap<>();
				columns.put("personId".toLowerCase(), Types.INTEGER);
				columns.put("movieId".toLowerCase(), Types.INTEGER);
				checkTable(con, "ProducedBy".toLowerCase(), columns, 636588, reasons, schemaIssues, dataIssues);
				
				columns = new HashMap<>();
				columns.put("personId".toLowerCase(), Types.INTEGER);
				columns.put("movieId".toLowerCase(), Types.INTEGER);
				checkTable(con, "WrittenBy".toLowerCase(), columns, 673083, reasons, schemaIssues, dataIssues);
				
				// Check FKs!
				int maxPersonId = getMaxId(con, "Person"), maxMovieId = getMaxId(con, "Movie"), maxGenreId = getMaxId(con, "Genre");
				if (maxGenreId > 0 && !forceFK(con, "INSERT INTO HasGenre(movieId, genreId) VALUES (?,?)", 1, maxGenreId)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in HasGenre table; ");
				}
				if (maxMovieId > 0 && !forceFK(con, "INSERT INTO HasGenre(movieId, genreId) VALUES (?,?)", maxMovieId, 1)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in HasGenre table; ");
				}
				if (maxMovieId > 0 && !forceFK(con, "INSERT INTO ActedIn(movieId, personId) VALUES (?,?)", maxMovieId, 1)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in ActedIn table; ");
				}
				if (maxMovieId > 0 && !forceFK(con, "INSERT INTO ComposedBy(movieId, personId) VALUES (?,?)", maxMovieId, 1)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in ComposedBy table; ");
				}
				if (maxMovieId > 0 && !forceFK(con, "INSERT INTO DirectedBy(movieId, personId) VALUES (?,?)", maxMovieId, 1)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in DirectedBy table; ");
				}
				if (maxMovieId > 0 && !forceFK(con, "INSERT INTO EditedBy(movieId, personId) VALUES (?,?)", maxMovieId, 1)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in EditedBy table; ");
				}
				if (maxMovieId > 0 && !forceFK(con, "INSERT INTO ProducedBy(movieId, personId) VALUES (?,?)", maxMovieId, 1)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in ProducedBy table; ");
				}
				if (maxMovieId > 0 && !forceFK(con, "INSERT INTO WrittenBy(movieId, personId) VALUES (?,?)", maxMovieId, 1)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in WrittenBy table; ");
				}
				if (maxPersonId > 0 && !forceFK(con, "INSERT INTO ActedIn(movieId, personId) VALUES (?,?)", 1, maxPersonId)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in ActedIn table; ");
				}
				if (maxPersonId > 0 && !forceFK(con, "INSERT INTO ComposedBy(movieId, personId) VALUES (?,?)", 1, maxPersonId)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in ComposedBy table; ");
				}
				if (maxPersonId > 0 && !forceFK(con, "INSERT INTO DirectedBy(movieId, personId) VALUES (?,?)", 1, maxPersonId)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in DirectedBy table; ");
				}
				if (maxPersonId > 0 && !forceFK(con, "INSERT INTO EditedBy(movieId, personId) VALUES (?,?)", 1, maxPersonId)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in EditedBy table; ");
				}
				if (maxPersonId > 0 && !forceFK(con, "INSERT INTO ProducedBy(movieId, personId) VALUES (?,?)", 1, maxPersonId)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in ProducedBy table; ");
				}
				if (maxPersonId > 0 && !forceFK(con, "INSERT INTO WrittenBy(movieId, personId) VALUES (?,?)", 1, maxPersonId)) {
					schemaIssues.set(true);
					reasons.append("No proper FK in WrittenBy table; ");
				}
				
				con.close();
			}
		} catch (Exception oops) {
			reasons.append("A major problem happened: " + oops.getMessage() + "; ");
			weirdIssues.set(true);
		} finally {
			builder = null;
			if (process != null)
				process.destroy();
		}
		
		int penalties = 0;
		if (weirdIssues.get() || notCompleted.get())
			penalties = -40;
		else if (notInTime.get())
			penalties = -15;
		else if (dataIssues.get())
			penalties = -10;
		if (schemaIssues.get())
			penalties = -10;
		
		System.out.println("Penalties: " + penalties);
		System.out.println("Reasons: " + reasons);
	}
	
	private static void checkTable(Connection con, String table, Map<String, Integer> columns, int expectedSize, StringBuffer reasons, 
			AtomicBoolean schemaIssues, AtomicBoolean dataIssues) throws Exception {
		PreparedStatement ps = con.prepareStatement("SELECT * FROM " + table + " LIMIT 1");
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				Integer type = columns.remove(rs.getMetaData().getColumnName(i).toLowerCase());
				if (type != null && rs.getMetaData().getColumnType(i) != type) {
					reasons.append("Column with unexpected type: " + rs.getMetaData().getColumnName(i) + " in table: " + table + "; ");
					schemaIssues.set(true);
				}
			}
			
			if (!columns.isEmpty()) {
				reasons.append("Expected columns not found: " + columns.keySet() + " in table: " + table + "; ");
				schemaIssues.set(true);
			}
		} else {
			reasons.append("Table " + table + " not found; ");
			schemaIssues.set(true);
		}
		rs.close();
		ps.close();
		
		ps = con.prepareStatement("SELECT COUNT(*) AS cnt FROM " + table);
		rs = ps.executeQuery();
		if (rs.next() && rs.getInt(1) != expectedSize) {
			reasons.append("Size of " + table + " was unexpected; ");
			dataIssues.set(true);
		}
		rs.close();
		ps.close();
	}
	
	private static int getMaxId(Connection con, String table) throws Exception {
		int max = -1;
		PreparedStatement ps = con.prepareStatement("SELECT MAX(id) AS m FROM " + table);
		ResultSet rs = ps.executeQuery();
		if (rs.next())
			max = rs.getInt(1) + 1;
		rs.close();
		ps.close();
		return max;
	}
	
	private static boolean forceFK(Connection con, String insert, int id1, int id2) throws Exception {
		boolean hasError = false;
		PreparedStatement ps = con.prepareStatement(insert);
		ps.setInt(1, id1);
		ps.setInt(2, id2);
		try {
			ps.executeUpdate();
		} catch (Exception oops) {
			hasError = oops.getMessage().contains("foreign");
		}
		return hasError;
	}

}
