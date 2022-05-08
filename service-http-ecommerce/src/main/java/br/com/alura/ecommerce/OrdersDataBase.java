package br.com.alura.ecommerce;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

import br.com.alura.ecommerce.database.LocalDataBase;

public class OrdersDataBase implements Closeable{

	private final LocalDataBase dataBase;

	public OrdersDataBase() throws SQLException {
		this.dataBase = new LocalDataBase("orders_database");
		this.dataBase.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key)");
	}

	public boolean saveNew(Order order) throws SQLException {
		if(wasProcessed(order)) {
			return false;
		}
		dataBase.update("insert into Orders (uuid) values (?)", order.getOrderId());
		return true;
	}
	
	 private boolean wasProcessed(Order order) throws SQLException {
	    	return dataBase.query("Select uuid from Orders where uuid = ? limit 1", order.getOrderId()).next();
		}

	@Override
	public void close() throws IOException {
		try {
			this.dataBase.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IOException();
		}
		
	}
}
