import java.util.*;
import java.io.*;

import java.io.FileInputStream;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

// Depend on library from https://poi.apache.org/
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.poifs.filesystem.OfficeXmlFileException;

class ReaderExcel extends ReaderUtil
{
	Iterator<Row> rows;

	public ReaderExcel(XML xml) throws Exception
	{
		super(xml);

		String filename = xml.getAttribute("filename");
		File file = new File(filename);
		if (!file.exists()) throw new FileNotFoundException("File not found: " + filename);
		if (instance == null) instance = file.getName();

		String sheetname = xml.getAttribute("worksheet");
		Sheet worksheet;
		try {
			FileInputStream fis = new FileInputStream(file);
			HSSFWorkbook workbook = new HSSFWorkbook(fis);
			worksheet = sheetname == null ? workbook.getSheetAt(0) : workbook.getSheet(sheetname);
		} catch(OfficeXmlFileException ex) {
			FileInputStream fis = new FileInputStream(file);
			XSSFWorkbook workbook = new XSSFWorkbook(fis);
			worksheet = sheetname == null ? workbook.getSheetAt(0) : workbook.getSheet(sheetname);
		}

		if (worksheet == null) throw new FileNotFoundException("Worksheet " + (sheetname == null ? "" : sheetname + " ") + "not found in file: " + filename);

		rows = worksheet.rowIterator();
		Row row = null;

		String startstr = xml.getAttribute("start_row");
		if (startstr != null)
		{
			int startrow = Integer.parseInt(startstr);
			while(rows.hasNext() && startrow > 1)
			{
				row = rows.next();
				startrow--;
			}
		}

		while(rows.hasNext())
		{
			row = rows.next();
			Cell cell = row.getCell(0);
			if (cell != null)
			{
				String value = getCellValue(cell);
				if (!value.isEmpty()) break;
			}
		}

		if (!rows.hasNext()) throw new AdapterException("Missing header row in sheet " + worksheet.getSheetName() + ": " + instance);

		headers = new LinkedHashSet<String>();
		Iterator<Cell> cells = row.iterator();
		boolean isempty = false;
		while (cells.hasNext())
		{
			Cell cell = cells.next();
			String value = getCellValue(cell);
			if (value.isEmpty())
			{
				isempty = true;
				continue;
			}
			else if (isempty)
				throw new AdapterException("Empty header values can only be at the end in sheet " + worksheet.getSheetName() + ": " + instance);
			headers.add(value);
		}
	}

	String getCellValue(Cell cell) throws Exception
	{
		CellType type = cell.getCellTypeEnum();
		String value = null;

		switch(type)
		{
			case STRING:
				value = cell.getStringCellValue();
				break;
			case BLANK:
				value = "";
				break;
			case NUMERIC:
				final DecimalFormat df = new DecimalFormat("0",DecimalFormatSymbols.getInstance(Locale.ENGLISH));
				df.setMaximumFractionDigits(340);
				value = df.format(cell.getNumericCellValue());
				break;
			case BOOLEAN:
				value = cell.getBooleanCellValue() ? "true" : "false";
				break;
			case FORMULA:
				value = cell.getCellFormula();
				break;
			default:
				throw new AdapterException("Unsupported cell type " + type);
		}

		if (Misc.isLog(35)) Misc.log("Cell value [" + cell.getRowIndex() + "," + cell.getColumnIndex() + "]: " + value);
		return value;
	}

	@Override
	public LinkedHashMap<String,String> nextRaw() throws Exception
	{
		if (!rows.hasNext()) return null;

		Row row = rows.next();
		LinkedHashMap<String,String> result = new LinkedHashMap<String,String>();
		int pos = 0;
		for(String header:headers)
		{
			if (!header.isEmpty())
			{
				Cell cell = row.getCell(pos);
				if (cell != null) result.put(header,getCellValue(cell));
			}
			pos++;
		}

		if (Misc.isLog(15)) Misc.log("row [excel]: " + result);
		return result;
	}
}
