//package com.idm;
package IMDBLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class IMDBLoader {
    public static final List<String> FILE_NAMES = Arrays.asList("name.basics.tsv.gz", "title.basics.tsv.gz", "title.ratings.tsv.gz", "title.principals.tsv.gz");
    public static final List<String> TITLE_TYPES = Arrays.asList("movie", "short", "tvShort", "tvMovie");
    private static final int STEP = 10000;

    public static String url = "jdbc:mysql://localhost:3306/mysql?rewriteBatchStatements=true";
    public static String user = "root";
    public static String password = "chaitanya";
    public static String pathToIMDBData = "";

    public static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS IMDB;";

    public static final String PERSONS_TABLE_CREATION = ""
            + "CREATE TABLE IF NOT EXISTS IMDB.Person ( "
            + "    id int, "
            + "    name varchar(255), "
            + "    birthYear int, "
            + "    deathYear int, "
            + "    PRIMARY KEY (id) "
            + ");";

    public static final String MOVIES_TABLE_CREATION = ""
            + "CREATE TABLE IF NOT EXISTS IMDB.Movie ( "
            + "    id int, "
            + "    title varchar(255), "
            + "    releaseYear int, "
            + "    runtime int, "
            + "    rating float, "
            + "    numberOfVotes int, "
            + "    PRIMARY KEY (id) "
            + ");";


    public static final String GENRES_TABLE_CREATION = ""
            + "CREATE TABLE IF NOT EXISTS IMDB.Genre ( "
            + "    id int, "
            + "    name varchar(255), "
            + "    PRIMARY KEY (id) "
            + ");";

    public static final String HAS_GENRE_TABLE_CREATION = ""
            + "CREATE TABLE IF NOT EXISTS IMDB.HasGenre ( "
            + "    genreId int, "
            + "    movieId int, "
            + "    PRIMARY KEY (genreId,movieId), "
            + "    FOREIGN KEY (genreId) REFERENCES IMDB.Genre(id), "
            + "    FOREIGN KEY (movieId) REFERENCES IMDB.Movie(id) "
            + ");";


    public static final String DIRECTED_BY_TABLE_CREATION = ""
            + "CREATE TABLE IF NOT EXISTS IMDB.DirectedBy ( "
            + "    personID int, "
            + "    movieId int, "
            + "    PRIMARY KEY (personID, movieId), "
            + "    FOREIGN KEY (personID) REFERENCES IMDB.Person(id), "
            + "    FOREIGN KEY (movieId) REFERENCES IMDB.Movie(id) "
            + ");";


    public static final String EDITED_BY_TABLE_CREATION = ""
            + "CREATE TABLE IF NOT EXISTS IMDB.EditedBy ( "
            + "    personID int, "
            + "    movieId int, "
            + "    PRIMARY KEY (personID, movieId), "
            + "    FOREIGN KEY (personID) REFERENCES IMDB.Person(id), "
            + "    FOREIGN KEY (movieId) REFERENCES IMDB.Movie(id) "
            + ");";


    public static final String PRODUCED_BY_TABLE_CREATION = ""
            + "CREATE TABLE IF NOT EXISTS IMDB.ProducedBy ( "
            + "    personID int, "
            + "    movieId int, "
            + "    PRIMARY KEY (personID, movieId), "
            + "    FOREIGN KEY (personID) REFERENCES IMDB.Person(id), "
            + "    FOREIGN KEY (movieId) REFERENCES IMDB.Movie(id) "
            + ");";


    public static final String WRITTEN_BY_TABLE_CREATION = ""
            + "CREATE TABLE IF NOT EXISTS IMDB.WrittenBy ( "
            + "    personID int, "
            + "    movieId int, "
            + "    PRIMARY KEY (personID,movieId), "
            + "    FOREIGN KEY (personID) REFERENCES IMDB.Person(id), "
            + "    FOREIGN KEY (movieId) REFERENCES IMDB.Movie(id) "
            + ");";


    public static final String COMPOSED_BY_TABLE_CREATION = ""
            + "CREATE TABLE IF NOT EXISTS IMDB.ComposedBy ( "
            + "    personID int, "
            + "    movieId int, "
            + "    PRIMARY KEY (personID,movieId), "
            + "    FOREIGN KEY (personID) REFERENCES IMDB.Person(id), "
            + "    FOREIGN KEY (movieId) REFERENCES IMDB.Movie(id) "
            + ");";


    public static final String ACTED_IN_TABLE_CREATION = ""
            + "CREATE TABLE IF NOT EXISTS IMDB.ActedIn ( "
            + "    personID int, "
            + "    movieId int, "
            + "    PRIMARY KEY (personID, movieId), "
            + "    FOREIGN KEY (personID) REFERENCES IMDB.Person(id), "
            + "    FOREIGN KEY (movieId) REFERENCES IMDB.Movie(id) "
            + ");";

    public static final String UPDATE_RATINGS_IN_MOVIE = "UPDATE IMDB.Movie SET rating = ?, numberOfVotes = ? WHERE (id = ?);";

    public static final String INSERT_MOVIE = "INSERT IGNORE INTO IMDB.Movie (id, title, releaseYear, runtime) VALUES (?, ?, ?, ?);";

    public static final String INSERT_HAS_GENRE_RELATION = "INSERT IGNORE INTO IMDB.HasGenre (genreId, movieId) VALUES (?, ?);";

    public static final String INSERT_GENRE = "INSERT IGNORE INTO IMDB.Genre (id, name) VALUES (?, ?);";

    public static final String INSERT_PERSON = "INSERT IGNORE INTO IMDB.Person (id, name, birthYear, deathYear) VALUES (?, ?, ?, ?);";

    public static final String INSERT_IN_DIRECTED_BY = "INSERT IGNORE INTO IMDB.DirectedBy (personID, movieId) VALUES (?, ?);";

    public static final String INSERT_IN_EDITED_BY = "INSERT IGNORE INTO IMDB.EditedBy (personID, movieId) VALUES (?, ?);";

    public static final String INSERT_IN_PRODUCED_BY = "INSERT IGNORE INTO IMDB.ProducedBy (personID, movieId) VALUES (?, ?);";

    public static final String INSERT_IN_WRITTEN_BY = "INSERT IGNORE INTO IMDB.WrittenBy (personID, movieId) VALUES (?, ?);";

    public static final String INSERT_IN_COMPOSED_BY = "INSERT IGNORE INTO IMDB.ComposedBy (personID, movieId) VALUES (?, ?);";

    public static final String INSERT_IN_ACTED_BY = "INSERT IGNORE INTO IMDB.ActedIn (personID, movieId) VALUES (?, ?);";

    public static void main(String[] args){
        long start = System.currentTimeMillis();

        //folderToAssignment = args[0];
        if(args.length>4){
            url = args[0];
            user = args[1];
            password = args[2];
            pathToIMDBData = args[3];
        }

        databaseSetup();
        //readNameBasics();
        long step1 = System.currentTimeMillis();
        System.out.println("Adding Persons took " + (step1-start)/(1000*60) + "mins");
        readTitleBasics();
        long step2 = System.currentTimeMillis();
        System.out.println("Adding Movies along with Genre table and their relations took " + (step2-step1)/(1000*60) + "mins");
        //updateTitleRatings();
        long step3 = System.currentTimeMillis();
        System.out.println("Updating ratings of movies took " + (step3-step2)/(1000*60) + "mins");
        //readTitlePrincipal();
        long end = System.currentTimeMillis();
        System.out.println("Adding principal cast relations took " + (end-step3)/(1000*60) + "mins");
        System.out.println("All the tasks took " + (end-start)/(1000*60) + "mins");
    }

    private static void updateTitleRatings() {
        /**Reading title.ratings file**/
        try{
            InputStream gzipStream = new GZIPInputStream(new FileInputStream(pathToIMDBData+FILE_NAMES.get(2)));
            Scanner sc = new Scanner(gzipStream, "UTF-8");
            sc.nextLine();

            Connection con = null;
            PreparedStatement st = null;
            try {
                con = DriverManager.getConnection(url, user, password);
                con.setAutoCommit(false);
                st = con.prepareStatement(UPDATE_RATINGS_IN_MOVIE);
                int count = 0;
                while (sc.hasNextLine()) {
                    String[] attributes = sc.nextLine().split("\\t");
                    st.setFloat(1, Float.parseFloat(attributes[1]));
                    st.setInt(2, castToInteger(attributes[2]));
                    st.setInt(3, Integer.parseInt(attributes[0].substring(2)));
                    st.addBatch();
                    count++;
                    if (count % STEP == 0) {
                        st.executeBatch();
                        con.commit();
                    }
                }
                st.executeBatch();
                con.commit();
                sc.close();
            }catch(SQLException oops){
                oops.printStackTrace();
            }
            finally {
                try {
                    if (st != null)
                        st.close();
                    if (con != null)
                        con.close();
                }catch(SQLException bigOops){
                    System.err.println("DB Error "+bigOops);
                }
            }
        }catch(IOException ioe){
            System.err.println("IO Exception "+ioe.getMessage());
        }
    }

    private static void databaseSetup() {
        //Create DB, tables.
        Connection con = null;
        PreparedStatement st = null;
        try {
            con = DriverManager.getConnection(url, user, password);
            con.setAutoCommit(false);
            st = con.prepareStatement(CREATE_DATABASE);
            st.executeUpdate();
            st = con.prepareStatement(PERSONS_TABLE_CREATION);
            st.executeUpdate();
            st = con.prepareStatement(MOVIES_TABLE_CREATION);
            st.executeUpdate();
            st = con.prepareStatement(GENRES_TABLE_CREATION);
            st.executeUpdate();
            st = con.prepareStatement(HAS_GENRE_TABLE_CREATION);
            st.executeUpdate();
            st = con.prepareStatement(DIRECTED_BY_TABLE_CREATION);
            st.executeUpdate();
            st = con.prepareStatement(EDITED_BY_TABLE_CREATION);
            st.executeUpdate();
            st = con.prepareStatement(PRODUCED_BY_TABLE_CREATION);
            st.executeUpdate();
            st = con.prepareStatement(WRITTEN_BY_TABLE_CREATION);
            st.executeUpdate();
            st = con.prepareStatement(COMPOSED_BY_TABLE_CREATION);
            st.executeUpdate();
            st = con.prepareStatement(ACTED_IN_TABLE_CREATION);
            st.executeUpdate();
            con.commit();
        }catch(SQLException oops){
            oops.printStackTrace();
        }
        finally {
            try {
                if (st != null)
                    st.close();
                if (con != null)
                    con.close();
            }catch(SQLException bigOops){
                System.err.println("DB Error "+bigOops);
            }
        }
    }

    private static void readTitlePrincipal() {
        try{
            InputStream gzipStream = new GZIPInputStream(new FileInputStream(pathToIMDBData+FILE_NAMES.get(3)));
            Scanner sc = new Scanner(gzipStream, "UTF-8");
            sc.nextLine();

            Connection con = null;
            PreparedStatement directorSt = null;
            PreparedStatement editorSt = null;
            PreparedStatement producerSt = null;
            PreparedStatement writerSt = null;
            PreparedStatement composerSt = null;
            PreparedStatement actorSt = null;

            int directorCount = 0;
            int editorCount = 0;
            int producerCount = 0;
            int writerCount = 0;
            int composerCount = 0;
            int actorCount = 0;
            try {
                con = DriverManager.getConnection(url, user, password);
                con.setAutoCommit(false);

                directorSt = con.prepareStatement(INSERT_IN_DIRECTED_BY);
                editorSt = con.prepareStatement(INSERT_IN_EDITED_BY);
                producerSt = con.prepareStatement(INSERT_IN_PRODUCED_BY);
                writerSt = con.prepareStatement(INSERT_IN_WRITTEN_BY);
                composerSt = con.prepareStatement(INSERT_IN_COMPOSED_BY);
                actorSt = con.prepareStatement(INSERT_IN_ACTED_BY);

                while (sc.hasNextLine()) {
                    String[] attributes = sc.nextLine().split("\\t");
                    switch (attributes[3]) {
                        case "director":
                            directorCount++;
                            insertInDirectedBy(attributes[0], attributes[2], directorSt);
                            if (directorCount % STEP == 0) {
                                directorSt.executeBatch();
                                con.commit();
                            }
                            break;
                        case "editor":
                            editorCount++;
                            insertInEditedBy(attributes[0], attributes[2], editorSt);
                            if (editorCount % STEP == 0) {
                                editorSt.executeBatch();
                                con.commit();
                            }
                            break;
                        case "producer":
                            producerCount++;
                            insertInProducedBy(attributes[0], attributes[2], producerSt);
                            if (producerCount % STEP == 0) {
                                producerSt.executeBatch();
                                con.commit();
                            }
                            break;
                        case "writer":
                            writerCount++;
                            insertInWrittenBy(attributes[0], attributes[2], writerSt);
                            if (writerCount % STEP == 0) {
                                writerSt.executeBatch();
                                con.commit();
                            }
                            break;
                        case "composer":
                            composerCount++;
                            insertInComposedBy(attributes[0], attributes[2], composerSt);
                            if (composerCount % STEP == 0) {
                                composerSt.executeBatch();
                                con.commit();
                            }
                            break;
                        case "actor":
                        case "actress":
                        case "self":
                            actorCount++;
                            insertInActedBy(attributes[0], attributes[2], actorSt);
                            if (actorCount % STEP == 0) {
                                actorSt.executeBatch();
                                con.commit();
                            }
                            break;
                        default:
                            break;
                        }
                    }
                directorSt.executeBatch();
                editorSt.executeBatch();
                producerSt.executeBatch();
                con.commit();
                writerSt.executeBatch();
                composerSt.executeBatch();
                actorSt.executeBatch();
                con.commit();
                sc.close();
            }catch(SQLException oops){
            oops.printStackTrace();
            }
            finally {
                try {
                    if (directorSt != null)
                        directorSt.close();
                    if (editorSt != null)
                        editorSt.close();
                    if (producerSt != null)
                        producerSt.close();
                    if (writerSt != null)
                        writerSt.close();
                    if (composerSt != null)
                        composerSt.close();
                    if (actorSt != null)
                        actorSt.close();
                    if (con != null)
                        con.close();
                }catch(SQLException bigOops){
                System.out.println("DB Error "+bigOops);
                }
            }
        }catch(IOException ioe){
            System.out.println("IO Exception "+ioe.getMessage());
        }
    }

    private static void insertInDirectedBy(String movieId, String personId, PreparedStatement directorSt) throws SQLException {
        directorSt.setInt(1, Integer.parseInt(personId.substring(2)));
        directorSt.setInt(2, Integer.parseInt(movieId.substring(2)));
        directorSt.addBatch();
    }

    private static void insertInEditedBy(String movieId, String personId, PreparedStatement editorSt) throws SQLException {
        editorSt.setInt(1, Integer.parseInt(personId.substring(2)));
        editorSt.setInt(2, Integer.parseInt(movieId.substring(2)));
        editorSt.addBatch();
    }

    private static void insertInProducedBy(String movieId, String personId, PreparedStatement producerSt) throws SQLException {
        producerSt.setInt(1, Integer.parseInt(personId.substring(2)));
        producerSt.setInt(2, Integer.parseInt(movieId.substring(2)));
        producerSt.addBatch();
    }

    private static void insertInWrittenBy(String movieId, String personId, PreparedStatement writerSt) throws SQLException {
        writerSt.setInt(1, Integer.parseInt(personId.substring(2)));
        writerSt.setInt(2, Integer.parseInt(movieId.substring(2)));
        writerSt.addBatch();
    }

    private static void insertInComposedBy(String movieId, String personId, PreparedStatement composerSt) throws SQLException {
        composerSt.setInt(1, Integer.parseInt(personId.substring(2)));
        composerSt.setInt(2, Integer.parseInt(movieId.substring(2)));
        composerSt.addBatch();
    }

    private static void insertInActedBy(String movieId, String personId, PreparedStatement actorSt) throws SQLException {
        actorSt.setInt(1, Integer.parseInt(personId.substring(2)));
        actorSt.setInt(2, Integer.parseInt(movieId.substring(2)));
        actorSt.addBatch();
    }

    private static void readTitleBasics() {
        Map<String,Integer> genreMap = new HashMap<>();
        try{
            InputStream gzipStream = new GZIPInputStream(new FileInputStream(pathToIMDBData+FILE_NAMES.get(1)));
            Scanner sc = new Scanner(gzipStream, "UTF-8");
            sc.nextLine();
            Connection con = null;
            PreparedStatement movieSt = null;
            PreparedStatement hasGenreSt = null;
            PreparedStatement genreSt = null;
            try {
                con = DriverManager.getConnection(url, user, password);
                con.setAutoCommit(false);
                int count = 0;
                Integer genreId = 1;
                movieSt = con.prepareStatement(INSERT_MOVIE);
                hasGenreSt = con.prepareStatement(INSERT_HAS_GENRE_RELATION);
                genreSt = con.prepareStatement(INSERT_GENRE);
                while (sc.hasNextLine()) {
                    String[] attributes = sc.nextLine().split("\\t");
                    if (TITLE_TYPES.contains(attributes[1])) {
                        Movie movie = new Movie();
                        movie.setMovieId(Integer.parseInt(attributes[0].substring(2)));
                        movie.setTitle(attributes[3]);
                        movie.setReleaseYear(castToInteger(attributes[5]));
                        movie.setRuntime(castToInteger(attributes[7]));
                        movie.setGenres(Arrays.asList(attributes[8].split(",")));
                        for(String genre:movie.getGenres()){
                            if(!genreMap.containsKey(genre)){
                                //Insert genre in table
                                insertGenre(genre, genreId, genreSt);
                                genreMap.put(genre, genreId);
                                genreId++;
                            }
                        }
                        //Insert movies in table
                        insertMovie(movie, movieSt);
                        //Insert has-genre relations
                        insertMovieGenreRelations(movie, genreMap, hasGenreSt);
                        count++;
                        if (count % STEP == 0) {
                            movieSt.executeBatch();
                            hasGenreSt.executeBatch();
                            con.commit();
                        }
                    }
                }
                movieSt.executeBatch();
                hasGenreSt.executeBatch();
                con.commit();
                sc.close();
            }catch(SQLException oops){
                oops.printStackTrace();
            }
            finally {
                try {
                    if (movieSt != null)
                        movieSt.close();
                    if (hasGenreSt != null)
                        hasGenreSt.close();
                    if (genreSt != null)
                        genreSt.close();
                    if (con != null)
                        con.close();
                }catch(SQLException bigOops){
                    System.err.println("DB Error "+bigOops);
                }
            }
        }catch(IOException ioe){
            System.err.println("IOException "+ioe.getMessage());
        }
    }

    private static void insertMovieGenreRelations(Movie movie, Map<String, Integer> genreMap, PreparedStatement hasGenreSt) throws SQLException {
            for (String movieGenre : movie.getGenres()) {
                Integer gid = genreMap.get(movieGenre);
                hasGenreSt.setInt(1, gid);
                hasGenreSt.setInt(2, movie.getMovieId());
                hasGenreSt.addBatch();
            }
    }

    private static void insertMovie(Movie movie, PreparedStatement st) throws SQLException {
        st.setInt(1, movie.getMovieId());
        st.setString(2, movie.getTitle());
        st.setObject(3, movie.getReleaseYear(), java.sql.JDBCType.INTEGER);
        st.setObject(4, movie.getRuntime(), java.sql.JDBCType.INTEGER);
        st.addBatch();
    }

    private static void insertGenre(String genre, Integer genreId, PreparedStatement genreSt) throws SQLException {
        genreSt.setInt(1, genreId);
        genreSt.setString(2, genre);
        genreSt.executeUpdate();
    }

    private static void readNameBasics() {
        try{
            InputStream gzipStream = new GZIPInputStream(new FileInputStream(pathToIMDBData+FILE_NAMES.get(0)));
            Scanner sc = new Scanner(gzipStream, "UTF-8");
            sc.nextLine();
            Connection con = null;
            PreparedStatement st = null;
            try{
                con = DriverManager.getConnection(url, user, password);
                con.setAutoCommit(false);
                int count = 0;
                st = con.prepareStatement(INSERT_PERSON);
                while (sc.hasNextLine()) {
                    String[] attributes = sc.nextLine().split("\\t");
                    Person person = new Person();
                    person.setPersonId(Integer.parseInt(attributes[0].substring(2)));
                    person.setName(attributes[1]);
                    person.setBirthYear(castToInteger(attributes[2]));
                    person.setDeathYear(castToInteger(attributes[3]));

                    insertPerson(person, st);
                    count++;

                    if (count % STEP == 0) {
                        st.executeBatch();
                        con.commit();
                    }
                }
                st.executeBatch();
                con.commit();
                sc.close();
        }catch(SQLException oops){
            oops.printStackTrace();
        }
        finally {
                    try {
                        if (st != null)
                            st.close();
                        if (con != null)
                            con.close();
                    }catch(SQLException bigOops){
                        System.err.println("DB Error "+bigOops);
                    }
         }
        }catch(IOException ioe){
            System.err.println("IOException "+ioe.getMessage());
        }
    }

    private static void insertPerson(Person person, PreparedStatement st) throws SQLException {
                st.setInt(1, person.getPersonId());
                st.setString(2, person.getName());
                st.setObject(3, person.getBirthYear(), java.sql.JDBCType.INTEGER);
                st.setObject(4, person.getDeathYear(), java.sql.JDBCType.INTEGER);
                st.addBatch();
    }

    private static Integer castToInteger(String attribute) {
        if (attribute!=null && attribute.matches("\\d+")){
            return Integer.parseInt(attribute);
        }
        else{
            return null;
        }
    }
}

class Movie {

    Integer movieId;

    String title;

    Integer releaseYear;

    Integer runtime;

    Float rating;

    Integer numberOfVotes;

    List<String> genres;

    public Integer getMovieId() {
        return movieId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Integer getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(Integer releaseYear) {
        this.releaseYear = releaseYear;
    }

    public Integer getRuntime() {
        return runtime;
    }

    public void setRuntime(Integer runtime) {
        this.runtime = runtime;
    }

    public List<String> getGenres() {
        return genres;
    }

    public void setGenres(List<String> genres) {
        this.genres = genres;
    }

    @Override
    public String toString() {
        return "Movie{" +
                "movieId='" + movieId + '\'' +
                ", title='" + title + '\'' +
                ", releaseYear=" + releaseYear +
                ", runtime=" + runtime +
                ", rating=" + rating +
                ", numberOfVotes=" + numberOfVotes +
                ", genres=" + genres +
                '}';
    }
}

class Person {

    Integer personId;

    String name;

    Integer birthYear;

    Integer deathYear;

    public Integer getPersonId() {
        return personId;
    }

    public void setPersonId(Integer personId) {
        this.personId = personId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getBirthYear() {
        return birthYear;
    }

    public void setBirthYear(Integer birthYear) {
        this.birthYear = birthYear;
    }

    public Integer getDeathYear() {
        return deathYear;
    }

    public void setDeathYear(Integer deathYear) {
        this.deathYear = deathYear;
    }

    @Override
    public String toString() {
        return "Person{" +
                "personId='" + personId + '\'' +
                ", name='" + name + '\'' +
                ", birthYear=" + birthYear +
                ", deathYear=" + deathYear +
                '}';
    }
}