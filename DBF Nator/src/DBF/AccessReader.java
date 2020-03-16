package DBF;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import javax.swing.JTextField;

import org.apache.commons.lang3.StringUtils;

public class AccessReader implements ActionListener{

	String columns[];
	JTextField csvJ;
	JTextField dbfJ;
	JTextField accdbJ;
	boolean pronto = false;
	
	String csv;
	String dbf;
	String accdb;

	public AccessReader(JTextField csvJ, JTextField dbfJ, JTextField accdbJ) {
		this.csvJ = csvJ;
		this.dbfJ = dbfJ;
		this.accdbJ = accdbJ;
	}

	@Override
	public void actionPerformed(ActionEvent ae) {
		csv = csvJ.getText() + "/";
		dbf = dbfJ.getText() + "/";
		accdb = accdbJ.getText() + "/";		
		//System.out.println(accdb.getText());
		Connection connection = null;
        Statement statement = null;
        //ResultSet resultSet = null;
        //StringBuilder column = new StringBuilder();	        
        ResultSetMetaData meta = null;
    	
        String bancos[]= {"BIGSPDBF","I4PRODBF","NBIGDBF","SPYDBF", "TIADBF"};
        String a = (";Memory=false");
        String files[] = {"201910","201911","201912"};
        String zeros = null;
        
        int i = 1;
        int j = 1;
        int cont = 0;
        int num = 0;
        long lines = 1;
        String linha = null;
        
        // Step 1: Loading or 
        // registering Oracle JDBC driver class
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
        try {
        while(new File(msAccDB).exists()) {      
            //String msAccDB = "C:/Users/ftsilva3/Documents/DBF/pronto/PREMREC201910.accdb";
            String dbURL = "jdbc:ucanaccess://" + msAccDB; 
 
            // Step 2.A: Create and 
            // get connection using DriverManager class
            System.out.println("------------------------------------");
        	System.out.println("Conectando ao arquivo " + files[num]);
        	System.out.println("------------------------------------");
            try {
            	Logger.getLogger("hsqldb.db").setLevel(Level.OFF);
				connection = DriverManager.getConnection(dbURL+a);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
            // Step 2.B: Creating JDBC Statement
            try {
				statement = connection.createStatement();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	System.out.println("------------------------------------");
        	System.out.println("Lendo arquivo " + files[num]);
        	System.out.println("------------------------------------");
        	
        	File arq = new File(csv + "DBF_PREMREC_DBF_" + files[num] + ".csv");
        	FileWriter fr = null;
			try {
				fr = new FileWriter(arq,true);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
            while(cont<bancos.length) {					
            	System.out.println("------------------------------------");
            	System.out.println("Select na tabela " + bancos[cont]);
            	System.out.println("------------------------------------");
            	
	            ResultSet rs = null;
				try {
					rs = statement.executeQuery("SELECT *FROM " + bancos[cont]);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	            try {
					meta = rs.getMetaData();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	            
	            if(!pronto) {
		            try {
						columns = new String[meta.getColumnCount()+1];
					} catch (SQLException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
						break;
					}
	            }
					try {
						while (rs.next()) {
							StringBuilder values = new StringBuilder();
							while(meta.getColumnCount() >= i) {								
								//System.out.println("column");
						      	//column.append(meta.getColumnName(i));	
								columns [0] = "SEQUENCIA";
						      	if (meta.getColumnName(i).equalsIgnoreCase("Ofer")) {
						      		//System.out.println("aqui");
						      		columns [i] = meta.getColumnName(i).toUpperCase();
						      		//System.out.println(columns[i]);
						      		i++;
						      		pronto = true;
						      		break;
						      	}
						      	//column.append(", ");
						      	columns [i] = meta.getColumnName(i).toUpperCase();
						      	i++;
						      	//System.out.println(column + " - " + (meta.getColumnCount() - i));
							}
							zeros = StringUtils.leftPad(Long.toString(lines), 10,"0");
							values.append("'" + zeros + "';");
							while(meta.getColumnCount()>=j) {
								//System.out.println("value");
								
								if(rs.getString(1) == "" || rs.getString(1).isBlank() || rs.getString(1).isEmpty()) {
									lines--;
									values.delete(0, values.length());
									break;
								}
								switch (j) {
									case 1: 
										linha = StringUtils.leftPad(rs.getString(j), 5,"0");
//		            				System.out.println(linha);
//		            				Thread.sleep(10000);
									break;
									case 2:
										linha = StringUtils.leftPad(rs.getString(j), 6,"0");
									break;
									case 3: 
										linha = StringUtils.leftPad(rs.getString(j), 3,"0");
									break;
									case 4:
										linha = StringUtils.leftPad(rs.getString(j), 4,"0");
									break;
									case 5: 
										linha = StringUtils.leftPad(rs.getString(j), 20,"0");
									break;
									case 6:
										linha = StringUtils.leftPad(rs.getString(j), 20,"0");
									break;
									case 7: 
										linha = StringUtils.leftPad(rs.getString(j), 20,"0");
									break;
									case 8:
										linha = StringUtils.leftPad(rs.getString(j), 8,"0");
									break;
									case 9: 
										linha = StringUtils.leftPad(rs.getString(j), 2,"0");
									break;
									case 10:
										linha = StringUtils.leftPad(rs.getString(j), 2,"0");
									break;
									case 11: 
										linha = StringUtils.leftPad(rs.getString(j), 8,"0");
									break;
									case 12:
										linha = StringUtils.leftPad(rs.getString(j), 8,"0");
									break;
									case 13: 
										linha = StringUtils.leftPad(rs.getString(j), 8,"0");
									break;
									case 14:
										linha = StringUtils.leftPad(rs.getString(j), 8,"0");
									break;
									case 15: 
										linha = StringUtils.leftPad(rs.getString(j), 16,"0");
									break;
									case 16:
										linha = StringUtils.leftPad(rs.getString(j), 16,"0");
									break;
									case 17: 
										linha = StringUtils.leftPad(rs.getString(j), 16,"0");
									break;
									case 18:
										linha = StringUtils.leftPad(rs.getString(j), 16,"0");
									break;
									case 19: 
										linha = StringUtils.leftPad(rs.getString(j), 16,"0");
									break;
									case 20:
										linha = StringUtils.leftPad(rs.getString(j), 1,"0");
									break;
								}
								
//			            		if (values.toString().contains("00000000000000010742"))
//				            		System.out.println(values.toString());
						    	values.append("'" + linha + "'");
//				            	if(i-j == 1) {
//				            		values.delete(0, values.length());
//				            		lines--;
//				            		j++;
//				            		break;
//				            	}
						    	values.append(";");
								j++;
							}
//			            	if (values.toString().contains("00000000000000010742"))
//			            		System.out.println(rs.getString(4) + " - " + rs.getString(5));
							//System.out.println("INSERT INTO DBF (" + column + ") VALUES (" + values + ")");
						    fr.flush();
						    //if (!rs.isLast())
						    //else
						    //	lines--;
						    if(!(rs.getString(1) == "") || rs.getString(1).isBlank() || rs.getString(1).isEmpty() || rs.getString(1) == "\r\n" || 
						    		rs.getString(1).toString() == "\r\n" || rs.getString(1).toString().isBlank() || rs.getString(1).toString().isEmpty() || 
						    		rs.getString(1).toString() == null || values.toString().isBlank() || values.toString().isEmpty() ||
						    		values.toString() == null || values.toString() == "" || values.toString() == "\r\n") {
						    	values.append("\r\n");
						    	if(!(values.length()<5))
						    		fr.append(values.toString());
						    	lines++;
						    }
						    else 
						    	System.out.println(lines + " - " + bancos[cont]);
							values.setLength(0);
							j=1;
							//Thread.sleep(4000);
							//System.out.println(rs.getString(1));
							//String sql = "INSERT INTO DBF (" + column + ") VALUES (" + RS.				
						}
					} catch (SQLException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	System.out.println("------------------------------------");
            	System.out.println("Select na tabela " + bancos[cont] + " terminado");
            	System.out.println("------------------------------------");
	            cont++;
	            //Thread.sleep(20000);
            }
            
            //System.out.println(column);
            
           // System.out.println(rs.getRow());
           // rs.getDate(i);
            cont = 0;

        	System.out.println("------------------------------------");
        	System.out.println("Leitura do arquivo " + files[num] + " terminado");
        	System.out.println("------------------------------------");
        	lines = 1;
        	try {
    			fr.close();
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
        	
            num++;
            try {
            	msAccDB = accdb + files[num] + ".accdb";
            }catch(Exception e) {
            	System.out.println("------------------------------------");
            	System.out.println("Csv's finalizado!");
            	System.out.println("------------------------------------");
            	break;
            }
         }
            // Step 3: Closing database connection
            try {
                if(null != connection) {
                    // cleanup resources, once after processing
                	//RS.close();
                    statement.close();
 
                    // and then finally close connection
                    connection.close();
                }
            }
            catch (SQLException sqlex) {
                sqlex.printStackTrace();
            }
        }
        catch(Exception ee) {
    		System.out.println("Deu ruim!");
    	}
		try {
			new DBFNator(csv, dbf, columns, bancos, files).dbfnator( /*fColmuns*/);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
}
}
