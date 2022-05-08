package br.com.alura.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {

            // we are not caring about any security issues, we are only
            // showing how to use http as a starting point
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var orderId = req.getParameter("uuid");

            var order = new Order(orderId, amount, email);
            
            try(var dataBase = new OrdersDataBase()){
            	if(dataBase.saveNew(order)) {
               	 orderDispatcher.send("ECOMMERCE_NEW_ORDER", email,
                    		new CorrelationId(NewOrderServlet.class.getSimpleName()),
                    		order);

                    System.out.println("New order sent successfully.");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New order sent");
               } else {
               	  System.out.println("Order already exists.");
                     resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                     resp.getWriter().println("Order already exists.");
               }
            }
            
            
            
           

        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


    }
}
