import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

public class IMDBLoader {
    static String folderToAssignment;
    static String dbUrl;
    static String pathToIMDBData;
    static String namebasics_filename;
    static String titlebasics_filename;
    static String titleratings_filename;
    static String titleprincipals_filename;

    public static void main(String[] args) throws ClassNotFoundException {

        dbUrl               = args.length> 0 ? args[0] : "jdbc:mysql://localhost:3306/loadimdb?rewriteBatchedStatements=true";
        String user         = args.length> 1 ? args[1] : "root";
        String password     = args.length> 2 ? args[2] : "shaunny";
        pathToIMDBData      = args.length> 3 ? args[3] : "/media/shaun/543C6D2D3C6D0B76/Users/Shaun/";
        setDataPaths();

        long start = System.currentTimeMillis();
        
        try (
                // Allocate a database 'Connection' object
                Connection conn = DriverManager.getConnection(dbUrl, user, password);   // For MySQL only
                // The format is: "jdbc:mysql://hostname:port/databaseName", "username", "password"
        ) {

            conn.setAutoCommit(false);

            /*createDatabase(conn);

            long time = System.currentTimeMillis();
            System.out.println("Database created in "+(time-start)/60000+" minutes.");*/
            
            //loadPersonTable(conn);

            long time2 = System.currentTimeMillis();
            System.out.println("Person created in "+(time2-start)/60000+" minutes.");

            //loadMovieTable(conn);

            long time = System.currentTimeMillis();
            System.out.println("Movie created in "+(time-time2)/60000+" minutes.");
            
            //loadRatings(conn);

            time2 = System.currentTimeMillis();
            System.out.println("Rating updated in "+(time2-time)/60000+" minutes.");

            loadTitlePrincipalsTable(conn);

            time = System.currentTimeMillis();
            System.out.println("Relations created in "+(time-time2)/60000+" minutes.");

        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Execution time is "+(System.currentTimeMillis()-start)/60000);
    }

    private static void setDataPaths() {
        namebasics_filename = pathToIMDBData+"name.basics.tsv.gz";
        titlebasics_filename = pathToIMDBData+"title.basics.tsv.gz";
        titleratings_filename = pathToIMDBData+"title.ratings.tsv.gz";
        titleprincipals_filename = pathToIMDBData+"title.principals.tsv.gz";
    }

    private static void createDatabase(Connection conn) {
        try {
            final String queryCreate = "create database if not exists LoadIMDB;";
            final String queryUse = "use LoadIMDB;";

            Statement stmt = conn.createStatement();

            stmt.execute(queryCreate);
            stmt.execute(queryUse);

            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void loadTitlePrincipalsTable(Connection conn) {

        try {
            final String createQueryActedIn = "create table if not exists actedIn (personId integer, movieId integer, " +
                    "primary key (personId, movieId), foreign key(personId) references person(id), " +
                    "foreign key(movieId) references movie(id));";

            final String createQueryComposedBy = "create table if not exists composedBy (personId integer, movieId integer, " +
                    "primary key (personId, movieId), foreign key(personId) references person(id), " +
                    "foreign key(movieId) references movie(id));";

            final String createQueryDirectedBy = "create table if not exists directedBy (personId integer, movieId integer, " +
                    "primary key (personId, movieId), foreign key(personId) references person(id), " +
                    "foreign key(movieId) references movie(id));";

            final String createQueryEditedBy = "create table if not exists editedBy (personId integer, movieId integer, " +
                    "primary key (personId, movieId), foreign key(personId) references person(id), " +
                    "foreign key(movieId) references movie(id));";

            final String createQueryProducedBy = "create table if not exists producedBy (personId integer, movieId integer, " +
                    "primary key (personId, movieId), foreign key(personId) references person(id), " +
                    "foreign key(movieId) references movie(id));";

            final String createQueryWrittenBy = "create table if not exists writtenBy (personId integer, movieId integer, " +
                    "primary key (personId, movieId), foreign key(personId) references person(id), " +
                    "foreign key(movieId) references movie(id));";

            Statement stmt = conn.createStatement();

            stmt.execute(createQueryActedIn);
            stmt.execute(createQueryComposedBy);
            stmt.execute(createQueryDirectedBy);
            stmt.execute(createQueryEditedBy);
            stmt.execute(createQueryProducedBy);
            stmt.execute(createQueryWrittenBy);

            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (
                InputStream  gzipStream_titleprincipals  =  new GZIPInputStream(new FileInputStream(titleprincipals_filename));
                Scanner sc_titleprincipals = new Scanner(gzipStream_titleprincipals,"UTF-8");
                ) {

            sc_titleprincipals.nextLine(); //Skipping the first line
            System.out.println("Creating relationships between person and movie table by parsing title.principals.tsv.gz");

            Statement stmt = conn.createStatement();
            String query;
            
            boolean insert = false;
            int count = 0;
            while (sc_titleprincipals.hasNext()){
                String rawLine_titlePrincipals = sc_titleprincipals.nextLine();
                String[] splitLine_titlePrincipals = rawLine_titlePrincipals.split("\t");
                String[] splitIndex_titlePrincipalsPerson = splitLine_titlePrincipals[0].split("(?<=\\D)(?=\\d)");
                String[] splitIndex_titlePrincipalsMovie = splitLine_titlePrincipals[2].split("(?<=\\D)(?=\\d)");
                //insert = false;

                if (splitLine_titlePrincipals[3].toLowerCase().contains("actor") ||
                        splitLine_titlePrincipals[3].toLowerCase().contains("actress") ||
                        splitLine_titlePrincipals[3].toLowerCase().contains("self")){

                    query = "insert ignore into actedIn (personId, movieId) SELECT person.id, movie.id FROM person, movie WHERE person.id = "+Integer.parseInt(splitIndex_titlePrincipalsPerson[1])+" and movie.id = " +Integer.parseInt(splitIndex_titlePrincipalsMovie[1]);
                    stmt.addBatch(query);
                    insert = true;
                    
                } else if (splitLine_titlePrincipals[3].toLowerCase().contains("composer")) {
                    
                	query = "insert ignore into composedBy (personId, movieId) SELECT person.id, movie.id FROM person, movie WHERE person.id = "+Integer.parseInt(splitIndex_titlePrincipalsPerson[1])+" and movie.id = " +Integer.parseInt(splitIndex_titlePrincipalsMovie[1]);
                    stmt.addBatch(query);
                    insert = true;
                    
                } else if (splitLine_titlePrincipals[3].toLowerCase().contains("director")) {
                    
                	query = "insert ignore into directedBy (personId, movieId) SELECT person.id, movie.id FROM person, movie WHERE person.id = "+Integer.parseInt(splitIndex_titlePrincipalsPerson[1])+" and movie.id = " +Integer.parseInt(splitIndex_titlePrincipalsMovie[1]);
                    stmt.addBatch(query);
                    insert = true;
                    
                } else if (splitLine_titlePrincipals[3].toLowerCase().contains("editor")) {
                    
                	query = "insert ignore into editedBy (personId, movieId) SELECT person.id, movie.id FROM person, movie WHERE person.id = "+Integer.parseInt(splitIndex_titlePrincipalsPerson[1])+" and movie.id = " +Integer.parseInt(splitIndex_titlePrincipalsMovie[1]);
                    stmt.addBatch(query);
                    insert = true;
                    
                } else if (splitLine_titlePrincipals[3].toLowerCase().contains("producer")) {
                    
                	query = "insert ignore into producedBy (personId, movieId) SELECT person.id, movie.id FROM person, movie WHERE person.id = "+Integer.parseInt(splitIndex_titlePrincipalsPerson[1])+" and movie.id = " +Integer.parseInt(splitIndex_titlePrincipalsMovie[1]);
                    stmt.addBatch(query);
                    insert = true;
                    
                } else if (splitLine_titlePrincipals[3].toLowerCase().contains("writer")) {
                    
                	query = "insert ignore into writtenBy (personId, movieId) SELECT person.id, movie.id FROM person, movie WHERE person.id = "+Integer.parseInt(splitIndex_titlePrincipalsPerson[1])+" and movie.id = " +Integer.parseInt(splitIndex_titlePrincipalsMovie[1]);
                    stmt.addBatch(query);
                    insert = true;
                    
                }

                if (insert) {
                    insert=false;
                    count++;
                    if (count%10000 == 0){
                        stmt.executeBatch();
                        conn.commit();
                        System.out.println("Processed & executed "+count+" entries.");
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            System.out.println("Processed & executed "+count+" entries.");

            stmt.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private static void loadRatings(Connection conn) {
        try (
                InputStream  gzipStream_titleratings  =  new GZIPInputStream(new FileInputStream(titleratings_filename));
                Scanner sc_titleratings = new Scanner(gzipStream_titleratings,"UTF-8");
                ) {

            sc_titleratings.nextLine(); //Skipping the first line

            System.out.println("Updating ratings from title.ratings.tsv");

            final String query = "update movie set rating = ?, numberOfVotes = ? where id = ?";
            PreparedStatement ps = conn.prepareStatement(query);

            int count = 0;
            while (sc_titleratings.hasNext()){

                String rawLine_titleRatings = sc_titleratings.nextLine();
                String[] splitLine_titleRatings = rawLine_titleRatings.split("\t");
                String[] splitIndex_titleRatings = splitLine_titleRatings[0].split("(?<=\\D)(?=\\d)");

                count++;
                ps.setFloat(1,Float.parseFloat(splitLine_titleRatings[1]));
                ps.setInt(2,Integer.parseInt(splitLine_titleRatings[2]));
                ps.setInt(3,Integer.parseInt(splitIndex_titleRatings[1]));

                ps.addBatch();
                //ps.clearParameters();

                if (count%10000 == 0){
                    ps.executeBatch();
                    conn.commit();
                    System.out.println("Processed & executed "+count+" entries.");
                }

            }
            ps.executeBatch();
            conn.commit();
            System.out.println("Processed & executed "+count+" entries.");

            ps.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void loadMovieTable(Connection conn) {
        System.out.println("Created table movie if it did not exist");
        createMovieTable(conn);

        System.out.println("Loading movie from file into database");
        insertMovieTable(conn);
    }

    private static void createMovieTable(Connection conn) {
        try {
            final String createQuery = "create table if not exists movie (id integer primary key, " +
                    "title varchar(50) not null, releaseYear integer, runtime integer, rating float, numberOfVotes integer);";

            final String createQueryGenre = "create table if not exists genre (id integer primary key, " +
                    "name varchar(50) not null);";

            final String createQueryHasGenre = "create table if not exists hasGenre (genreId integer, movieId integer, " +
                    "foreign key(genreId) references genre(id), foreign key(movieId) references movie(id));";

            Statement stmt = conn.createStatement();

            stmt.execute(createQuery);
            stmt.execute(createQueryGenre);
            stmt.execute(createQueryHasGenre);

            stmt.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertMovieTable(Connection conn) {
        try (
                InputStream  gzipStream_titlebasics  =  new GZIPInputStream(new FileInputStream(titlebasics_filename));
                Scanner sc_titlebasics = new Scanner(gzipStream_titlebasics,"UTF-8");
        ) {
            final String query = "insert ignore into movie (id, title, releaseYear, runtime) values (?,?,?,?)";
            PreparedStatement ps = conn.prepareStatement(query);

            final String queryGenre = "insert ignore into genre (id, name) values (?,?)";
            PreparedStatement ps_genre = conn.prepareStatement(queryGenre);

            final String queryHasGenre = "insert ignore into hasGenre (genreId, movieId) values (?,?)";
            PreparedStatement ps_hasgenre = conn.prepareStatement(queryHasGenre);

            sc_titlebasics.nextLine();  //Skipping the first line

            int id = 0;

            while (sc_titlebasics.hasNext()){
                String rawLine = sc_titlebasics.nextLine();
                String[] splitLine = rawLine.split("\t");
                if (splitLine[1].contains("movie") || splitLine[1].contains("short") ||
                        splitLine[1].contains("tvShort") || splitLine[1].contains("tvMovie")){

                    id++;

                    String[] splitIndex = splitLine[0].split("(?<=\\D)(?=\\d)");
                    ps.setInt(1, Integer.parseInt(splitIndex[1]));

                    checkAndUpdateGenre(ps_genre, ps_hasgenre, splitIndex[1], splitLine[8]);

                    if (splitLine[2].length() < 50) {
                        ps.setString(2,splitLine[2]);
                    } else {
                        ps.setString(2,splitLine[2].substring(0,50));
                    }

                    if (!splitLine[5].contains("\\N")) {

                        ps.setInt(3,Integer.parseInt(splitLine[5]));

                    } else {
                        ps.setInt(3,0);
                    }

                    if (!splitLine[7].contains("\\N")) {
                        ps.setInt(4,Integer.parseInt(splitLine[7]));
                    } else {
                        ps.setInt(4,0);
                    }

                    ps.addBatch();

                    if (id%50000 == 0){
                        ps.executeBatch();
                        ps_genre.executeBatch();
                        ps_hasgenre.executeBatch();
                        conn.commit();
                        System.out.println("Processed & executed "+id+" entries.");
                    }
                }

            }
            ps.executeBatch();
            ps_genre.executeBatch();
            ps_hasgenre.executeBatch();
            conn.commit();
            System.out.println("Processed & executed "+id+" entries.");

            ps.close();
            ps_genre.close();
            ps_hasgenre.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    static List<String> genreInMemory = new ArrayList<>();
    private static void checkAndUpdateGenre(PreparedStatement ps_genre, PreparedStatement ps_hasgenre,
                                            String movieIndex, String csvGenre) {
        String[] genres = csvGenre.split(",");
        for (String genre : genres) {
            if ((!genreInMemory.contains(genre))) {
                genreInMemory.add(genre);
                try {
                    ps_genre.setInt(1, genreInMemory.size());
                    ps_genre.setString(2, genre);
                    ps_genre.addBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            try {
                ps_hasgenre.setInt(1, genreInMemory.indexOf(genre)+1);
                ps_hasgenre.setInt(2, Integer.parseInt(movieIndex));
                ps_hasgenre.addBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return;
    }

    private static void loadPersonTable(Connection conn) {

        System.out.println("Created table person if it did not exist");
        createPersonTable(conn);

        System.out.println("Loading person from file into database");
        insertPersonTable(conn);

    }

    private static void createPersonTable(Connection conn) {
        try {
            final String createQuery = "create table if not exists person (id integer primary key, " +
                    "name varchar(50) not null, birthYear integer, deathYear integer);";

            Statement stmt = conn.createStatement();
            stmt.execute(createQuery);
            /*CREATE TABLE IF NOT EXISTS person
            (
                    id        INTEGER PRIMARY KEY,
                    name      VARCHAR(50) NOT NULL,
            birthyear INTEGER NOT NULL,
            deathyear INTEGER
            );*/
            stmt.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertPersonTable(Connection conn) {
        try (
                InputStream  gzipStream  =  new GZIPInputStream(new FileInputStream(namebasics_filename));
                Scanner sc = new Scanner(gzipStream,"UTF-8");
        ) {
            final String query = "insert ignore into person (id, name, birthYear, deathYear) values (?,?,?,?)";
            PreparedStatement ps = conn.prepareStatement(query);
            sc.nextLine(); //Skipping the first line
            int id = 0;
            while (sc.hasNext()){
                String rawLine = sc.nextLine();
                id++;
                String[] splitLine = rawLine.split("\t");

                ps.setInt(1,id);
                if (splitLine[1].length() < 50) {
                    ps.setString(2, splitLine[1]);
                } else {
                    ps.setString(2, splitLine[1].substring(0,50));
                }
                //ps.setString(2,splitLine[1]);
                if (!splitLine[2].contains("\\N")) {
                    ps.setInt(3,Integer.parseInt(splitLine[2]));
                } else {
                    ps.setInt(3,0);
                }
                //ps.setString(3,Integer.parseInt(splitLine[2]));
                if (!splitLine[3].contains("\\N")) {
                    ps.setInt(4,Integer.parseInt(splitLine[3]));
                } else {
                    ps.setInt(4,0);
                }
                //ps.setString(4,Integer.parseInt(splitLine[3]));
                //ps.execute();
                ps.addBatch();

                if (id%10000 == 0){
                    ps.executeBatch();
                    conn.commit();
                    System.out.println("Processed & executed "+id+" entries.");
                }
            }
            ps.executeBatch();
            conn.commit();
            System.out.println("Processed & executed "+id+" entries.");

            ps.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

}
