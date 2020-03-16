package DBF;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFWriter;

public class DBFNator {
	
	String csv;
	String dbf;
	String colunas[];
	String bancos[];
	String files[];
	
	public DBFNator(String csv, String dbf, String[] colunas, String[] bancos, String [] files) {
		this.colunas = colunas;
		this.bancos = bancos;
		this.csv = csv;
		this.dbf = dbf;
		this.files = files;
	}

	void dbfnator() throws FileNotFoundException {	
		int i = 0;
		while (new File(csv + "DBF_PREMREC_DBF_" + files[i] + ".csv").exists()) {
			String linha[] = null;
			//int i = 0;
			int j = 0;
			DBFField fields[] = new DBFField[colunas.length]; 
			//try {
				System.out.println("------------------------------------");
            	System.out.println("Gerando DBF" + files[i]);
            	System.out.println("------------------------------------");
            	
				DBFWriter writer = new DBFWriter(new File(dbf + "DBF_PREMREC_DBF_" + files[i] + ".dbf"));			
				Scanner scanner = new Scanner(new File(csv + "DBF_PREMREC_DBF_" + files[i] + ".csv"));
				Object rowData[] = new Object[colunas.length];
				linha = scanner.nextLine().split(";");
				for (j = 0; j < linha.length; j++) {
					//linha[j].toString().replace("'", "");
					fields[j] = new DBFField();
					fields[j].setName(colunas[j]);
					fields[j].setType(DBFDataType.CHARACTER);
					fields[j].setLength(linha[j].toString().replace("'", "").length());
					rowData[j] = linha[j].toString().replace("'", "");
					//rowData[j] = linha[j].toString();
				}
				
				writer.setFields(fields);
				//System.out.println(linha[0].toString().replace("'", ""));
				
				writer.addRecord(rowData);
				j=0;
				while(scanner.hasNextLine()) {
					linha = scanner.nextLine().split(";");				
					for (j = 0; j < linha.length; j++) {
						rowData[j] = linha[j].toString().replace("'", "");
					}
					//System.out.println(rowData.toString());
					writer.addRecord(rowData);
				}
				scanner.close();
				writer.close();
				
				System.out.println("------------------------------------");
            	System.out.println("DBF" + files[i] + " Gerado com sucesso");
            	System.out.println("------------------------------------");
            	
            	i++;
            	try {
            		new File(csv + "DBF_PREMREC_DBF_" + files[i] + ".csv");
            	}catch(Exception e) {
				System.out.println("------------------------------------");
            	System.out.println("Operação finalizada!");
            	System.out.println("------------------------------------");
            	break;
			}
		}
	}
}
