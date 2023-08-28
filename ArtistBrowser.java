/*THIS CODE WAS MY OWN WORK, IT WAS WRITTEN WITHOUT CONSULTING CODE WRITTEN BY OTHER STUDENTS
 * OR COPIED FROM ONLINE SOURCES
 * JOSHUA QIAN
 */
import java.util.ArrayList;
import java.sql.*;
import java.util.Collections;
import java.io.*;
import java.util.*;

public class ArtistBrowser {

	/* A connection to the database */
	private Connection connection;

	/**
	 * Constructor loads the JDBC driver. No need to modify this.
	 */
	public ArtistBrowser() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("Failed to locate the JDBC driver.");
		}
	}

	/**
	* Establishes a connection to be used for this session, assigning it to
	* the private instance variable 'connection'.
	*
	* @param  url       the url to the database
	* @param  username  the username to connect to the database
	* @param  password  the password to connect to the database
	* @return           true if the connection is successful, false otherwise
	*/
	public boolean connectDB(String url, String username, String password) {
		try {
			this.connection = DriverManager.getConnection(url, username, password);
			return true;
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
			return false;
		}
	}

	/**
	* Closes the database connection.
	*
	* @return true if the closing was successful, false otherwise.
	*/
	public boolean disconnectDB() {
		try {
			this.connection.close();
		return true;
		} catch (SQLException e) {
			System.out.println("SQL Exception: " + e.getMessage());
			return false;
		}
	}


	/**
	 * Returns a sorted list of the names of all musicians who were part of a band
	 * at some point between a given start year and an end year (inclusive).
 	 *
	 * Returns an empty list if no musicians match, or if the given timeframe is invalid.
	 *
	 * NOTE:
	 *    Use Collections.sort() to sort the names in ascending
	 *    alphabetical order.
	 *		Use prepared statements.
	 *
	 * @param startYear
	 * @param endYear
	 * @return  a sorted list of artist names
	 */
	public ArrayList<String> findArtistsInBands(int startYear, int endYear) {
		ArrayList<String> answer = new ArrayList<>() ;
		if (startYear > endYear) {
			System.out.println("Error: Start Year is later than End Year");
        	return answer;
		}
		
		try {
			String queryString = "SELECT name " + 
								"FROM Artist A NATURAL JOIN Role R NATURAL JOIN WasInBand W " +
								"WHERE R.role = 'Musician' AND start_year <= ? AND end_year >= ?";

			PreparedStatement prepstmt = connection.prepareStatement(queryString);
			prepstmt.setInt(1, endYear);
			prepstmt.setInt(2, startYear);

			ResultSet rs = prepstmt.executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();
			int cols = rsmd.getColumnCount();
			answer = new ArrayList<String>(cols);
			while (rs.next()) {
				for (int i = 1; i <= cols; i++) {
					answer.add(rs.getString(i));
				}
			}

			rs.close();
			prepstmt.close();

			Collections.sort(answer);
            return answer;
			} catch (SQLException e) {
				System.out.println("SQL Exception." + "<Message>: " + e.getMessage());
			}
        	return answer;
	}



	/**
	 * Returns a sorted list of the names of all musicians and bands
	 * who released at least one album in a given genre.
	 *
	 * Returns an empty list if no such genre exists or no artist matches.
	 *
	 * NOTE:
	 *    Use Collections.sort() to sort the names in ascending
	 *    alphabetical order.
	 *		Use prepared statements.
	 *
	 * @param genre  the genre to find artists for
	 * @return       a sorted list of artist names
	 */
	public ArrayList<String> findArtistsInGenre(String genre) {
		ArrayList<String> answer = new ArrayList<>();
		try {
			String queryString = 	"SELECT name " +
									"FROM Artist A NATURAL JOIN Role R " +
									"WHERE role IN " +
										"(SELECT role FROM Role WHERE role = 'Musician' OR role ='Band') " +
										"AND artist_id IN " +
    									"(SELECT artist_id " +
										"FROM Album NATURAL JOIN Genre " +
										"WHERE genre = ?)";
			
			PreparedStatement prepstmt = connection.prepareStatement(queryString);
			prepstmt.setString(1, genre);
			ResultSet rs = prepstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int cols = rsmd.getColumnCount();
			answer = new ArrayList<String>(cols);
			while (rs.next()) {
				for (int i = 1; i <= cols; i++) {
					answer.add(rs.getString(i));
				}
			}

			rs.close();
			prepstmt.close();

			Collections.sort(answer);
            return answer;
		} catch (SQLException e) {
			System.out.println("SQL Exception." + "<Message>: " + e.getMessage());
		}
        return answer;
	}



	/**
	 * Returns a sorted list of the names of all collaborators
	 * (either as a main artist or guest) for a given artist.
	 *
	 * Returns an empty list if no such artist exists or the artist
	 * has no collaborators.
	 *
	 * NOTE:
	 *    Use Collections.sort() to sort the names in ascending
	 *    alphabetical order.
	 *		Use prepared statements.
	 *
	 * @param artist  the name of the artist to find collaborators for
	 * @return        a sorted list of artist names
	 */
	public ArrayList<String> findCollaborators(String artist) {
		ArrayList<String> answer = new ArrayList<>();
		try {
			String queryString = 	"SELECT name " + 
									"FROM Artist " + 
									"WHERE artist_id IN " + 
										"((SELECT artist2 as artist_id " + 
										"FROM Artist, Collaboration " + 
										"WHERE name = ? AND artist_id = artist1) " + 
										"UNION " + 
										"(SELECT artist1 as artist_id " + 
										"FROM Artist, Collaboration " + 
										"WHERE name = ? AND artist_id = artist2))";
			
			PreparedStatement prepstmt = connection.prepareStatement(queryString);
			prepstmt.setString(1, artist);
			prepstmt.setString(2, artist);

			ResultSet rs = prepstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int cols = rsmd.getColumnCount();

			answer = new ArrayList<String>(cols);
			while (rs.next()) {
				for (int i = 1; i <= cols; i++) {
					answer.add(rs.getString(i));
				}
			}

			rs.close();
			prepstmt.close();

			Collections.sort(answer);
            return answer;
		} catch (SQLException e) {
            System.out.println("SQL Exception." + "<Message>: " + e.getMessage());
        }
        return answer;
	}



	/**
	 * Returns a sorted list of the names of all songwriters
	 * who wrote songs for a given artist (the given artist is excluded).
	 *
	 * Returns an empty list if no such artist exists or the artist
	 * has no other songwriters other than themself.
	 *
	 * NOTE:
	 *    Use Collections.sort() to sort the names in ascending
	 *    alphabetical order.
	 *
	 * @param artist  the name of the artist to find the songwriters for
	 * @return        a sorted list of songwriter names
	 */
	public ArrayList<String> findSongwriters(String artist) {
		ArrayList<String> answer = new ArrayList<>();
		try {
			String queryString = 	"SELECT Ar.name AS songwriters " + 
									"FROM Song s, Artist Ar, Artist Ar2, BelongsToAlbum B, Album Al " +
            						"WHERE S.songwriter_id = Ar.artist_id AND S.song_id = B.song_id " +
									"AND B.album_id = Al.album_id "+
            						"AND Al.artist_id = Ar2.artist_id AND Ar2.name = ? AND Ar.artist_id <> Ar2.artist_id";

			PreparedStatement prepstmt = connection.prepareStatement(queryString);
			prepstmt.setString(1, artist);

			ResultSet rs = prepstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int cols = rsmd.getColumnCount();
			answer = new ArrayList<String>(cols);
			while (rs.next()) {
				for (int i = 1; i <= cols; i++) {
					answer.add(rs.getString(i));
				}
			}

			rs.close();
			prepstmt.close();

			Collections.sort(answer);
            return answer;
		} catch (SQLException e) {
			System.out.println("SQL Exception." + "<Message>: " + e.getMessage());
		}
        return answer;
	}


	/**
	 * Returns a sorted list of the names of all common acquaintances
	 * for a given pair of artists.
	 *
	 * Returns an empty list if either of the artists does not exist,
	 * or they have no acquaintances.
	 *
	 * NOTE:
	 *    Use Collections.sort() to sort the names in ascending
	 *    alphabetical order.
	 *
	 * @param artist1  the name of the first artist to find acquaintances for
	 * @param artist2  the name of the second artist to find acquaintances for
	 * @return         a sorted list of artist names
	 */
	public ArrayList<String> findCommonAcquaintances(String artist1, String artist2) {
		ArrayList<String> answer = new ArrayList<>();
		try {
			//find collaborators and songwriters that have worked with each artist
			String queryString = 	"((SELECT name AS acquaintance " + 
									"FROM Artist " + 
									"WHERE artist_id IN " + 
										"((SELECT artist2 AS artist_id " + 
										"FROM Artist, Collaboration " + 
										"WHERE name = ? AND artist_id = artist1) " + 
										"UNION " + 
										"(SELECT artist1 AS artist_id " + 
										"FROM Artist, Collaboration " + 
										"WHERE name = ? AND artist_id = artist2))) " +
									"UNION " +
									"(SELECT name AS acquaintance " + 
									"FROM Artist " + 
									"WHERE artist_id IN " +
										"(SELECT songwriter_id " + 
										"FROM Artist Ar, Album Al, BelongsToAlbum B, Song S " + 
										"WHERE Ar.artist_id = Al.artist_id AND " +
											"Al.album_id = B.album_id AND " + 
											"B.song_id = S.song_id AND " + 
											"name = ? AND S.songwriter_id <> Ar.artist_id))) " +
									"INTERSECT " +
									"((SELECT name AS acquaintance " + 
									"FROM Artist " + 
									"WHERE artist_id IN " + 
										"((SELECT artist2 AS artist_id " + 
										"FROM Artist, Collaboration " + 
										"WHERE name = ? AND artist_id = artist1) " + 
										"UNION " + 
										"(SELECT artist1 AS artist_id " + 
										"FROM Artist, Collaboration " + 
										"WHERE name = ? AND artist_id = artist2))) " +
									"UNION " +
									"(SELECT name AS acquaintance " + 
									"FROM Artist " + 
									"WHERE artist_id IN " +
										"(SELECT songwriter_id " + 
										"FROM Artist Ar, Album Al, BelongsToAlbum B, Song S " + 
										"WHERE Ar.artist_id = Al.artist_id AND " +
											"Al.album_id = B.album_id AND " + 
											"B.song_id = S.song_id AND " + 
											"name = ? AND S.songwriter_id <> Ar.artist_id)))";
			
			PreparedStatement prepstmt = connection.prepareStatement(queryString);
			prepstmt.setString(1, artist1);
			prepstmt.setString(2, artist1);
			prepstmt.setString(3, artist1);
			prepstmt.setString(4, artist2);
			prepstmt.setString(5, artist2);
			prepstmt.setString(6, artist2);

			ResultSet rs = prepstmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			int cols = rsmd.getColumnCount();
			answer = new ArrayList<String>(cols);
			while (rs.next()) {
				for (int i = 1; i <= cols; i++) {
					answer.add(rs.getString(i));
				}
			}

			rs.close();
			prepstmt.close();

			Collections.sort(answer);
            return answer;
		} catch (SQLException e) {
			System.out.println("SQL Exception." + "<Message>: " + e.getMessage());
		}
        return answer;
	}

	/**
	 * Returns true if two artists have a collaboration path connecting
	 * them in the database (see A3 handout for our definition of a path).
	 * For example, artists `Z' and `Usher' are considered connected even though
	 * they have not collaborated directly on any song, because 'Z' collaborated
	 * with `Alicia Keys' who in turn had collaborated with `Usher', therefore there
	 * is a collaboration path connecting `Z' and `Usher'.
	 *
	 * Returns false if there is no collaboration path at all between artist1 and artist2
	 * or if either of them do not exist in the database.
	 *
	 * @return    true iff artist1 and artist2 have a collaboration path connecting them
	 */
	public boolean artistConnectivity(String artist1, String artist2) {
		boolean isConnected = false;
		try {
		   String queryString = "SELECT artist_id FROM artist WHERE name = ?";
		   PreparedStatement prepstmt = connection.prepareStatement(queryString);
		   prepstmt.setString(1, artist1);
		   ResultSet rs = prepstmt.executeQuery();
		   int artist_id1 = 0;
		   while(rs.next()){
			  artist_id1 = rs.getInt(1);
		   }
		   prepstmt.setString(1, artist2);
		   rs = prepstmt.executeQuery();
		   int artist_id2 = 0;
		   while(rs.next()){
			  artist_id2 = rs.getInt(1);
		   }
  
		   isConnected = areArtistsConnected(connection, artist_id1, artist_id2);
  
		   rs.close();
		   prepstmt.close();
		} catch (SQLException e) {
			System.out.println("SQL Exception." + "<Message>: " + e.getMessage());
		}
		return isConnected;
	 }
  
  
	 private static boolean areArtistsConnected(Connection conn, int artist1_id, int artist2_id) throws SQLException {
		Map<Integer, Integer> visitedArtists = new HashMap<>();
		Integer prevArtist = 0;
		return artistsConnectedCalculator(conn, artist1_id, artist2_id, visitedArtists,prevArtist);
	 }
	 

	 //search "neighboring" artists until a path between two artists is found or deemed to be non-existent
	 private static boolean artistsConnectedCalculator(Connection conn, int artist1Name, int artist2Name, Map<Integer, Integer> visitedArtists,Integer prevArtist) throws SQLException {
  
		String queryString = "SELECT artist1, artist2 FROM Collaboration WHERE artist1= ? OR artist2= ? ";
		PreparedStatement prepstmt = conn.prepareStatement(queryString);
		prepstmt.setInt(1, artist1Name);
		prepstmt.setInt(2, artist1Name);
		ResultSet rs = prepstmt.executeQuery();

		while (rs.next()) {
		   if (visitedArtists.get(artist1Name) != null && visitedArtists.get(artist1Name) == artist2Name) {
			  rs.close();
			  prepstmt.close();	
			  return true;
  
			//to prevent looping
		   }else if (visitedArtists.get(artist1Name) != null && visitedArtists.get(artist1Name) != prevArtist){
			  continue;
		   }
		   Integer artist = rs.getInt(1) == artist1Name ? rs.getInt(2) : rs.getInt(1);
			if(prevArtist == artist){
			  rs.close();
			  prepstmt.close();
			  return false;
		  	 }
			 //move to next "neighboring" artist
		   prevArtist = artist;
		   visitedArtists.put(rs.getInt(1), rs.getInt(2));
		   if (artist.equals(artist2Name)) {
			  rs.close();
			  prepstmt.close();
			  return true;
		   } else {
			  boolean isNextArtistConnected = artistsConnectedCalculator(conn, artist, artist2Name, visitedArtists, prevArtist);
			  if (isNextArtistConnected) {
				 rs.close();
				 prepstmt.close();
				 return true;
			  }
		   }
		}
		rs.close();
		prepstmt.close();
		return false;
	 }
  


	public static void main(String[] args) {

		if( args.length < 2 ){
			System.out.println("Usage: java ArtistBrowser <userName> <password>");
			return;
		}

		String user = args[0];
		String pass = args[1];

		ArtistBrowser a3 = new ArtistBrowser();

		String url = "jdbc:postgresql://localhost:1738/postgres?currentSchema=artistDB";
		a3.connectDB(url, user, pass);

		System.err.println("\n----- ArtistsInBands -----");
    ArrayList<String> res = a3.findArtistsInBands(1990,1999);
    for (String s : res) {
      System.err.println(s);
    }

		System.err.println("\n----- ArtistsInGenre -----");
    res = a3.findArtistsInGenre("Rock");
    for (String s : res) {
      System.err.println(s);
    }

		System.err.println("\n----- Collaborators -----");
		res = a3.findCollaborators("Usher");
		for (String s : res) {
		  System.err.println(s);
		}

		System.err.println("\n----- Songwriters -----");
	        res = a3.findSongwriters("Justin Bieber");
		for (String s : res) {
		  System.err.println(s);
		}

		System.err.println("\n----- Common Acquaintances -----");
		res = a3.findCommonAcquaintances("Jaden Smith", "Miley Cyrus");
		for (String s : res) {
		  System.err.println(s);
		}

		System.err.println("\n----- artistConnectivity -----");
		String a1 = "Z", a2 = "Usher";
		boolean areConnected = a3.artistConnectivity(a1, a2);
		System.err.println("Do artists " + a1 + " and " + a2 + " have a collaboration path connecting them? Answer: " + areConnected);

		a3.disconnectDB();
	}
}
