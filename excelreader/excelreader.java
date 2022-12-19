import java.util.*;
import java.io.*;
import java.nio.file.Path;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

// Depend on library from https://poi.apache.org/
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.poifs.filesystem.OfficeXmlFileException;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;

class ReaderExcel extends ReaderUtil
{
	Iterator<Row> rows;

	public ReaderExcel(XML xml) throws AdapterException
	{
		setXML(xml);

		String filename = xml.getAttribute("filename");
		Set<Path> paths = Misc.glob(filename);
		if (paths.size() != 1) throw new AdapterNotFoundException("File not found or multiple match: " + filename);
		File file = new File(paths.iterator().next().toString());
		if (instance == null) instance = file.getName();

		Workbook workbook;
		try {
			try(FileInputStream fis = new FileInputStream(file)) {
				workbook = new HSSFWorkbook(fis);
			} catch(OfficeXmlFileException ex) {
				try(FileInputStream fis = new FileInputStream(file)) {
					workbook = new XSSFWorkbook(fis);
				}
			}
		} catch(IOException ex) {
			throw new AdapterException(ex);
		}

		String sheetname = xml.getAttribute("worksheet");
		Sheet worksheet = null;
		if (sheetname == null)
			worksheet = workbook.getSheetAt(0);
		else
		{
			int count = workbook.getNumberOfSheets();
			for(int x = 0;x <= workbook.getNumberOfSheets();x++)
			{
				String name = workbook.getSheetAt(x).getSheetName();
				if (sheetname.equals(name) || Misc.matches(name,sheetname))
				{
					worksheet = workbook.getSheetAt(x);
					break;
				}
			}
		}

		if (worksheet == null) throw new AdapterNotFoundException("Worksheet " + (sheetname == null ? "" : "\"" + sheetname + "\" ") + "not found in file: " + filename);

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
			row = null;
		}

		if (row == null) throw new AdapterException("Missing header row in sheet " + worksheet.getSheetName() + ": " + instance);
		if (!rows.hasNext()) throw new AdapterException("Missing data row in sheet " + worksheet.getSheetName() + ": " + instance);

		headers = new LinkedHashSet<>();
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

	String getCellValue(Cell cell) throws AdapterException
	{
		if (cell == null) return null;

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
				if (HSSFDateUtil.isCellDateFormatted(cell))
				{
					Date date = cell.getDateCellValue();
					value = Misc.getGmtDateFormat().format(date);
				} else {
					final DecimalFormat df = new DecimalFormat("0",DecimalFormatSymbols.getInstance(Locale.ENGLISH));
					df.setMaximumFractionDigits(340);
					value = df.format(cell.getNumericCellValue());
				}
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

		if (Misc.isLog(35)) Misc.log("Cell value [" + cell.getRowIndex() + "," + cell.getColumnIndex() + "]: " + type + ": " + value);
		return value;
	}

	@Override
	public Map<String,String> nextRaw() throws AdapterException
	{
		if (!rows.hasNext()) return null;

		Row row = rows.next();
		Map<String,String> result = new LinkedHashMap<>();
		int pos = 0;

		for(String header:headers)
		{
			if (!header.isEmpty())
			{
				Cell cell = row.getCell(pos);
				String value = getCellValue(cell);
				result.put(header,value == null ? "" : value);
			}
			pos++;
		}

		if (Misc.isLog(15)) Misc.log("row [excel]: " + result);
		return result;
	}
}
