package br.com.alura.ecommerce.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LocalDataBase {
	private final Connection connection;

    public LocalDataBase(String name) throws SQLException {
        String url = "jdbc:sqlite:target/"+ name + ".db";
        connection = DriverManager.getConnection(url);
      
    }
    
    public void createIfNotExists(String sql) {
    	  try {
              connection.createStatement().execute(sql);
          } catch(SQLException ex) {
              // be careful, the sql could be wrong, be reallllly careful
              ex.printStackTrace();
          }
    }

	public void update(String statment, String ... params) throws SQLException {
		prepare(statment, params).execute();
	}

	public ResultSet query(String statment, String ... params) throws SQLException {
		return prepare(statment, params).executeQuery();
	}
	
	 private PreparedStatement prepare(String statement, String[] params) throws SQLException {
	        var preparedStatement = connection.prepareStatement(statement);
	        for (int i = 0; i < params.length; i++) {
	            preparedStatement.setString(i + 1, params[i]);
	        }
	        return preparedStatement;
	    }

	public void close() throws SQLException {
		connection.close();
	}
}
