package oracle;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import utils.WtUtils;

/**
 * WANGYWE 2020-02-20
 * 
 * @author WANGYWE
 * 
 * ��CLOB�����ֶε������ļ�
 *
 */

public class ClobToString {
	
	
	public static void main(String args[]) throws SQLException, IOException{
		ClobToString c = new ClobToString();
		
		Connection iteconn = WtUtils.getJcybconn();
		
		Statement itestmt =iteconn.createStatement();
		StringBuffer sql = new StringBuffer("");
		String where = "",fileName = null;
		
		fileName = "C:\\wyw_batchexport_config_sub_info.sql";
		
		//where = "YXQ_Q='202001'";
		
		where = "  and (yxq_Q ='202001' or gnmk_dm like 'SBFJLJFHSFX_QTFX%') "; 
		
		sql.append("select * from batchexport_config_sub_info  ");
		sql.append(" where (head_sql is not null and data_sql is not null) ");
		sql.append(where);
		sql.append("order by bblx,gnmk_dm,yxq_q,yxq_z");
        ResultSet iters = itestmt.executeQuery(sql.toString());

        Clob clob;
        String gnmkDm,bblx,s,yxqQ;
        int i=0;
        
        File f=new File(fileName);//�½�һ���ļ���������������򴴽�һ�����ļ�
        FileWriter fw;
        fw=new FileWriter(f);
        
        //OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream("c:\\wyw.sql", true), "GBK");
    	
        
        while(iters.next()){
        	i++;
        	if(i%10==1){
        		System.out.println("DECLARE\r"
					     + "    tmp1_clob CLOB;\r"
					     + "    tmp2_clob CLOB;\r"
					     + "BEGIN\n");
        		s = "DECLARE\r"
					     + "    tmp1_clob CLOB;\r"
					     + "    tmp2_clob CLOB;\r"
					     + "BEGIN\n";
        		//c.write(s,f);
        		fw.write(s);//���ַ���д�뵽ָ����·���µ��ļ���
        	}
        	//��ӡDATA_SQL
        	clob = iters.getClob("DATA_SQL");
        	s = c.ClobToString(clob,"    tmp1_clob := '");
        	System.out.println(s);
        	//c.write(s,f);
        	fw.write(s);//���ַ���д�뵽ָ����·���µ��ļ���
        	//��ӡHEAD_SQL
        	clob = iters.getClob("HEAD_SQL");
        	s = c.ClobToString(clob,"    tmp2_clob := '");
        	System.out.println(s);
        	//c.write(s,f);
    		fw.write(s);//���ַ���д�뵽ָ����·���µ��ļ���
        	//��ӡUPDATE
        	gnmkDm = iters.getString("GNMK_DM");
        	bblx   = iters.getString("BBLX");
        	yxqQ   = iters.getString("YXQ_Q");
        	
        	s= "    UPDATE BATCHEXPORT_CONFIG_SUB_INFO SET DATA_SQL = tmp1_clob, HEAD_SQL = tmp2_clob WHERE "
        	 + "GNMK_DM = '"+gnmkDm+"' AND BBLX = '"+bblx+"' and YXQ_Q='"+yxqQ+"';\n";
        	System.out.println(s);
        	//c.write(s,f);
    		fw.write(s);//���ַ���д�뵽ָ����·���µ��ļ���
        	System.out.println();
        	//c.write(s,f);
    		fw.write("");//���ַ���д�뵽ָ����·���µ��ļ���
        	if(i%10==0){
        		System.out.println("END;\r"
					             + "/\r"
					             + "COMMIT;\r");
        		s = "END;\r"
	             + "/\r"
	             + "COMMIT;\r";
        		//c.write(s,f);
        		fw.write(s);//���ַ���д�뵽ָ����·���µ��ļ���
        	}
        }
        
        if(i%10!=0){
        	System.out.println("END;\r"
		             + "/\r"
		             + "COMMIT;\r");
        	s = "END;\r"
   	             + "/\r"
   	             + "COMMIT;\r";
        	//c.write(s,f);
    		fw.write(s);//���ַ���д�뵽ָ����·���µ��ļ���
        }
        
        
        fw.close();
		
	}
	// ����CLOBת��STRING���� 
    public String ClobToString(Clob clob,String header) throws SQLException, IOException { 
    	
        String reString = ""; 
        java.io.Reader is = clob.getCharacterStream();// �õ��� 
        BufferedReader br = new BufferedReader(is); 
        String s = br.readLine(); 
        StringBuffer sb = new StringBuffer(); 
        //����һ��
        if(s !=null){
        	//�������滻��˫����
        	s = s.replaceAll("\'", "\''");
        	s = s.replaceAll("&", "&'||'");
        	sb.append(header);
        	sb.append(s);
        	
        }
        s = br.readLine();
        if(s != null){
        	sb.append("' || \r"); 
        }else{
        	sb.append("';\n"); 
        }
        while (s != null) {// ִ��ѭ�����ַ���ȫ��ȡ����ֵ��StringBuffer��StringBufferת��STRING 
        	s = s.replaceAll("\'", "\''");
        	s = s.replaceAll("&", "&'||'");
        	sb.append("        chr(13) || '");
        	sb.append(s);
            s = br.readLine();
            if(s!=null){
            	sb.append("' || \r"); 
            }else{
            	sb.append("';\n"); 
            }
        } 
        reString = sb.toString(); 
        return reString; 
    } 
    

}
