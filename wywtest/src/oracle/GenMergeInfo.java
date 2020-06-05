package oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import utils.WtUtils;
import ctais.services.data.DataWindow;
import ctais.util.StringEx;

public class GenMergeInfo {
	
	public static void main(String args[]) throws Exception{
		
		String sql = "select * from BATCHEXPORT_CONFIG_SUB_INFO where  "
				+" gnmk_dm='SBFJLJFHSFX_QTFX_001' and BBLX='SBFZXQ_SJ' ";
		
		Connection conn = WtUtils.getJcybconn();
		
		Statement stmt = conn.createStatement();
		
		ResultSet rs = stmt.executeQuery(sql);
		
		rs.getRow();
		
	
		
		DataWindow dw = DataWindow.dynamicCreate(sql);

		dw.retrieve();
		
		String headSql = StringEx.sNull(dw.getItemAny(0, "HEAD_SQL"));
		dw = DataWindow.dynamicCreate(headSql);
		dw.retrieve();
	  
		
		GenMergeInfo gen = new GenMergeInfo();
		gen.genMergInfo(dw);
		
	}
	
	/**
     * @author  WANGYWE
     * @see 根据表头sql生成merge_info
     * @param headDw
     * @return
     */
    private String genMergInfo(DataWindow headDw){
    	long rows = headDw.getRowCount();
    	long cols = headDw.getColumnCount();
    	String value,next;
    	StringBuffer mergeInfo = new StringBuffer("");
    	StringBuffer mergeLine;
    	String last;
    	for(int i=0;i<rows;i++){
    		mergeLine = new StringBuffer("");
    		for(int j=0;j<cols-1;j++){
    			value = StringEx.sNull(headDw.getItemAny(i, j)).trim();
    			next = StringEx.sNull(headDw.getItemAny(i, j+1)).trim();
    			//last为 mergeLine的最后一个字符
				last = mergeLine.substring(mergeLine.length()-1);
    			if(next.equals(value)){
    				if(!"-".equals(last)){
	    				mergeLine.append(i+"-"+i+":");
	    				mergeLine.append(j+"-");
    				}
    			}else{
    				if("-".equals(last)){
    					mergeLine.append(j-1+",");
    				}
    			}
    		}
    		mergeInfo.append(mergeLine);
    	}
    	
    	System.out.println(mergeInfo.toString());
    	
    	return null;
    }

}
