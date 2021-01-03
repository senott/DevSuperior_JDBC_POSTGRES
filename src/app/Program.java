package app;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import db.DB;
import entities.Order;
import entities.OrderStatus;
import entities.Product;

public class Program {

	public static void main(String[] args) throws SQLException {
		
		Connection conn = DB.getConnection();
	
		Statement st = conn.createStatement();
			
		ResultSet rs = st.executeQuery("select * from products");

		System.out.println("\r\nProducts:");

		while (rs.next()) {
			Product p = instantiateProduct(rs);

			System.out.println(p);
		}

		ResultSet rsOrder = st.executeQuery("select * from orders");

		System.out.println("\r\nOrders:");

		while (rsOrder.next()) {
			Order o = instantiateOrder(rsOrder);

			System.out.println(o);
		}

		ResultSet rsOrderProducts = st.executeQuery("SELECT * FROM orders " +
				"INNER JOIN order_products ON orders.id = order_products.order_id " +
				"INNER JOIN products ON products.id = order_products.product_id");

		Map<Long, Order> orderMap = new HashMap<>();

		Map<Long, Product> productMap = new HashMap<>();

		System.out.println("\r\nOrders & Products:");

		while (rsOrderProducts.next()) {
			Long orderId = rsOrderProducts.getLong("order_id");

			if (orderMap.get(orderId) == null) {
				Order o = instantiateOrder(rsOrderProducts);

				orderMap.put(orderId, o);
			}

			Long productId = rsOrderProducts.getLong("product_id");

			if (productMap.get(productId) == null) {
				Product p = instantiateProduct(rsOrderProducts);

				productMap.put(productId, p);
			}

			orderMap.get(orderId).getProducts().add(productMap.get(productId));
		}

		for (Long itemId: orderMap.keySet()) {
			System.out.println(orderMap.get(itemId));

			for (Product p : orderMap.get(itemId).getProducts()) {
				System.out.println(p);
			}
			System.out.println();
		}
	}

	private static Boolean rsHasColumn(ResultSet rs, String columnName) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int columns = rsmd.getColumnCount();

		for (int i = 1; i <= columns; i++) {
			if (columnName.equals(rsmd.getColumnName(i))) {
				return true;
			}
		}

		return false;
	}

	private static Product instantiateProduct(ResultSet rs) throws SQLException {
		Product p = new Product();

		p.setId(rsHasColumn(rs, "product_id") ? rs.getLong("product_id") : rs.getLong("id"));
		p.setDescription(rs.getString("description"));
		p.setName(rs.getString("name"));
		p.setImageUri(rs.getString("image_uri"));
		p.setPrice(rs.getDouble("price"));

		return p;
	}

	private static Order instantiateOrder(ResultSet rs) throws SQLException {
		Order o = new Order();

		o.setId(rsHasColumn(rs, "order_id") ? rs.getLong("order_id") : rs.getLong("id"));
		o.setLatitude(rs.getDouble("latitude"));
		o.setLongitude(rs.getDouble("longitude"));
		o.setMoment(rs.getTimestamp("moment").toInstant());
		o.setStatus(OrderStatus.values()[rs.getInt("status")]);

		return o;
	}
}
