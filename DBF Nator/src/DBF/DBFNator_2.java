package DBF;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.swing.JTextField;

public class DBFNator_2 implements ActionListener{

	JTextField csvJ;
	JTextField dbfJ;
	JTextField accdbJ;
	
	String csv;
	String dbf;
	String accdb;

	public DBFNator_2(JTextField csvJ, JTextField dbfJ, JTextField accdbJ) {
		this.csvJ = csvJ;
		this.dbfJ = dbfJ;
		this.accdbJ = accdbJ;
	}


	@Override
	public void actionPerformed(ActionEvent e) {
		csv = csvJ.getText() + "/";
		dbf = dbfJ.getText() + "/";
		accdb = accdbJ.getText() + "/";	
		String bancos[]= {"BIGSPDBF","I4PRODBF","NBIGDBF","SPYDBF", "TIADBF"};
        String a = (";Memory=false");
        String files[] = {"201910","201911","201912"};
        String dbURL;        
        int num = 0;
        int cont = 0;
        int i= 0;
        Connection connection;
        Statement statement = null;
        ResultSetMetaData meta = null;
        boolean pronto = false;
        String columns[] = null;
        
        try {
        	 
            Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
        }
        catch(ClassNotFoundException cnfex) {
 
            System.out.println("Problem in loading or "
                    + "registering MS Access JDBC driver");
            cnfex.printStackTrace();
        }
 
        // Step 2: Opening database connection
        String msAccDB = accdb + files[num] + ".accdb";
        
        while(new File(msAccDB).exists()) {
        	dbURL = "jdbc:ucanaccess://" + msAccDB; 
        	 try {
 				connection = DriverManager.getConnection(dbURL+a);
 			} catch (SQLException e1) {
 				// TODO Auto-generated catch block
 				e1.printStackTrace();
 			} 
        }
		
		while(cont<bancos.length) {
			ResultSet rs = null;
			try {
				rs = statement.executeQuery("SELECT *FROM " + bancos[cont]);
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
            try {
				meta = rs.getMetaData();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			if(!pronto)
				try {
					columns = new String[meta.getColumnCount()+1];
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			try {
				while(rs.next()) {
					
					while(meta.getColumnCount() >= i) {			
						columns [0] = "SEQUENCIA";
				      	if (meta.getColumnName(i).equalsIgnoreCase("Ofer")) {
				      		columns [i] = meta.getColumnName(i).toUpperCase();
				      		i++;
				      		pronto = true;
				      		break;
				      	}
				      	columns [i] = meta.getColumnName(i).toUpperCase();
				      	i++;
					}
					
				}
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		
		}
	}
}
