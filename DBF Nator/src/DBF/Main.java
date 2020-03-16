package DBF;

import java.awt.Dimension;

//import java.io.FileNotFoundException;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

public class Main {

	public static void main(String[] args)  {
////	String fColmuns [] = {"SEQUENCIA", "COD_CIA", "DT_BASE", "TIPO_MOV", "COD_RAMO",
////	"NUM_APOL", "NUM_END", "NUM_PROP", "DT_PROP", "PRESTACAO", "QTDE_PREST",
////	"DT_EMIS_PRE", "DT_VEN_PRE", "DT_INI_VIG", "DT_FIM_VIG", "PR_EMIT", "PERC_COSS",
////	"AD_FRAC", "CUST_APOL", "IOF", "OFER"}; usado para debugar
//
////String colunas[] = new AccessReader().run();
		
		JFrame janela = new JFrame("DBFNator 3000");
		JLabel infoCsv = new JLabel("Informe o caminho para salvar o csv:");
		JTextField csv = new JTextField(70);
		JLabel infoDbf = new JLabel("Informe o caminho para salvar o dbf:");
		JTextField dbf = new JTextField(70);
		JLabel infoAccdb = new JLabel("Informe o caminho para onde está o access:");
		JTextField accdb = new JTextField(70);		
		
		JButton exe = new JButton("Executar");
		exe.addActionListener(new AccessReader(csv, dbf, accdb));
		
		JPanel painel = new JPanel();
		painel.add(infoCsv);
		painel.add(csv);
		painel.add(infoDbf);
		painel.add(dbf);
		painel.add(infoAccdb);
		painel.add(accdb);
		
		painel.add(exe);
		janela.add(painel);
		janela.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		janela.setPreferredSize(new Dimension(1080, 720));
		janela.pack();
		janela.setVisible(true);
	}
}

//C:/Users/ftsilva3/Documents