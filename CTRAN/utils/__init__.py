import pandas as pd

def transformCell(cell):
    return '<Cell ss:StyleID="s62"><Data ss:Type="String">{}</Data></Cell>'.format(cell)

def transformEAMxml(_list):
    xml_header='''<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
xmlns:o="urn:schemas-microsoft-com:office:office"
xmlns:x="urn:schemas-microsoft-com:office:excel"
xmlns:dt="uuid:C2F41010-65B3-11d1-A29F-00AA00C14882"
xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet"
xmlns:html="http://www.w3.org/TR/REC-html40">
<DocumentProperties xmlns="urn:schemas-microsoft-com:office:office">
<Author>Spenser Sutinen</Author>
<LastAuthor>Spenser Sutinen</LastAuthor>
<Created>2023-05-08T20:34:53Z</Created>
<Version>16.00</Version>
</DocumentProperties>
<CustomDocumentProperties xmlns="urn:schemas-microsoft-com:office:office">
<MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_Enabled dt:dt="string">true</MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_Enabled>
<MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_SetDate dt:dt="string">2023-05-08T20:37:05Z</MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_SetDate>
<MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_Method dt:dt="string">Standard</MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_Method>
<MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_Name dt:dt="string">defa4170-0d19-0005-0004-bc88714345d2</MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_Name>
<MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_SiteId dt:dt="string">f08eec6c-9496-4f32-bdf1-97e8d57b23dc</MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_SiteId>
<MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_ActionId dt:dt="string">2bd78961-bd08-4efb-8bbd-6df524463bc6</MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_ActionId>
<MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_ContentBits dt:dt="string">0</MSIP_Label_5cc27fc0-0b98-48ce-bc8f-333d0b1453a7_ContentBits>
</CustomDocumentProperties>
<OfficeDocumentSettings xmlns="urn:schemas-microsoft-com:office:office">
<AllowPNG/>
</OfficeDocumentSettings>
<ExcelWorkbook xmlns="urn:schemas-microsoft-com:office:excel">
<WindowHeight>11985</WindowHeight>
<WindowWidth>28800</WindowWidth>
<WindowTopX>32767</WindowTopX>
<WindowTopY>32767</WindowTopY>
<ProtectStructure>False</ProtectStructure>
<ProtectWindows>False</ProtectWindows>
</ExcelWorkbook>
<Styles>
<Style ss:ID="Default" ss:Name="Normal">
<Alignment ss:Vertical="Bottom"/>
<Borders/>
<Font ss:FontName="Calibri" x:Family="Swiss" ss:Size="11" ss:Color="#000000"/>
<Interior/>
<NumberFormat/>
<Protection/>
</Style>
<Style ss:ID="s62">
<NumberFormat ss:Format="@"/>
</Style>
</Styles> 
<Worksheet ss:Name="Sheet1">
  <Table ss:ExpandedColumnCount="3" ss:ExpandedRowCount="2" x:FullColumns="1"
   x:FullRows="1" ss:DefaultRowHeight="15">
   <Row ss:AutoFitHeight="0">
    <Cell ss:StyleID="s62"><Data ss:Type="String">2041</Data></Cell>
    <Cell ss:StyleID="s62"><Data ss:Type="String">[KEY]</Data></Cell>
    <Cell><Data ss:Type="String">[GROUPROW]</Data></Cell>
   </Row>
   <Row ss:AutoFitHeight="0">
    <Cell><Data ss:Type="String">[i]</Data></Cell>
    <Cell><Data ss:Type="String">test</Data></Cell>
    <Cell><Data ss:Type="String">[238:1;Sheet2;1:1]</Data></Cell>
   </Row>
  </Table>
  <WorksheetOptions xmlns="urn:schemas-microsoft-com:office:excel">
   <PageSetup>
    <Header x:Margin="0.3"/>
    <Footer x:Margin="0.3"/>
    <PageMargins x:Bottom="0.75" x:Left="0.7" x:Right="0.7" x:Top="0.75"/>
   </PageSetup>
   <Unsynced/>
   <Panes>
    <Pane>
     <Number>3</Number>
     <ActiveRow>1</ActiveRow>
     <ActiveCol>2</ActiveCol>
    </Pane>
   </Panes>
   <ProtectObjects>False</ProtectObjects>
   <ProtectScenarios>False</ProtectScenarios>
  </WorksheetOptions>
 </Worksheet>
<Worksheet ss:Name="Sheet2">
<Table ss:ExpandedColumnCount="7" ss:ExpandedRowCount="{}" x:FullColumns="1"
x:FullRows="1" ss:DefaultRowHeight="15">
<Row ss:AutoFitHeight="0">
    <Cell ss:StyleID="s62"><Data ss:Type="String">2041</Data></Cell>
    <Cell ss:StyleID="s62"><Data ss:Type="String">[KEY]</Data></Cell>
    <Cell ss:StyleID="s62"><Data ss:Type="String">238:2</Data></Cell>
    <Cell ss:StyleID="s62"><Data ss:Type="String">238:14</Data></Cell>
    <Cell ss:StyleID="s62"><Data ss:Type="String">238:7</Data></Cell>
    <Cell ss:StyleID="s62"><Data ss:Type="String">238:9</Data></Cell>
    <Cell ss:StyleID="s62"><Data ss:Type="String">238:3</Data></Cell>
</Row>
        '''.format(len(_list)+1)
    xml_footer='''
</Table>
<WorksheetOptions xmlns="urn:schemas-microsoft-com:office:excel">
<PageSetup>
    <Header x:Margin="0.3"/>
    <Footer x:Margin="0.3"/>
    <PageMargins x:Bottom="0.75" x:Left="0.7" x:Right="0.7" x:Top="0.75"/>
</PageSetup>
<Unsynced/>
<Selected/>
<TopRowVisible>1</TopRowVisible>
<Panes>
    <Pane>
    <Number>3</Number>
    <ActiveRow>8</ActiveRow>
    <ActiveCol>2</ActiveCol>
    </Pane>
</Panes>
<ProtectObjects>False</ProtectObjects>
<ProtectScenarios>False</ProtectScenarios>
</WorksheetOptions>
</Worksheet>
</Workbook>
    '''

    return xml_header + "\n".join(_list) + xml_footer
