###############################################
# Title: LODES Outflow Analysis
# Author: Chad Ramos  https://www.linkedin.com/in/chad-ramos
# Description: This script automates the process of extracting data and performing an analysis based on LODES data similar to the Census Bureau's On the Map tool.
# Pseudocode:
# 1 set input parameters, including current ArcPro project, and path for living atlas layer
# 2 Get user input study area and check for polygon
# 3 Add census data to map
# 4 Extract list of census blocks within study area by geoid
# 5 EXTRACT DATA FROM LODES.CSV, FILTER BY STUDY AREA LIST, CREATE REQUIRED DBFS
# 6 From extracted LODES data, sum and group by on work census block
# 7 Join data to census block shapefile
# 8 ADD DATA AND SYMBOLIZE



###############################################
# IMPORT LIBRARIES
import arcpy
from arcpy import conversion
from arcpy import management
from arcpy import analysis
import sys
import os
import pandas as pd

# INITIAL SETUP
aprx = arcpy.mp.ArcGISProject("CURRENT") #Set project to the currently open project where the tool is being run
m = aprx.activeMap #Set map object to variable with activeMap method
arcpy.env.overwriteOutput=True

# INPUT PATHS
#dataPath = r'https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/US_Census_Blocks_v1/FeatureServer' #If your study area is outside of Texas, use this path but be warned that its every census block in the US and takes considerable processing power. ALSO SEE LINE 131!!
dataPath = r'https://services.arcgis.com/P3ePLMYs2RVChkJx/arcgis/rest/services/Texas_Census_2020_Redistricting_Blocks/FeatureServer' #path for living atlas layer

#TEMP OUTPUTS
tempout=arcpy.env.scratchGDB

#PERMANENT OUTPUTS
defaultGDB=aprx.defaultGeodatabase
outcsv=os.path.join(defaultGDB, "outcsv.csv") #this is created outside a geoprocessing tool and will not default to the GDB
#outcsv=r'C:\Users\cramos\OneDrive - City of Kyle\Documents\2024\August\LODES_Script_Tool\Data\outcsv.csv'
outcsv2=os.path.join(defaultGDB, "outcsv2.csv")
outcsv3=os.path.join(defaultGDB, "outcsv3.csv")
outcsv4=os.path.join(defaultGDB, "outcsv4.csv")
outcsv444=os.path.join(defaultGDB, "outcsv444.csv")

#1 INPUT PARAMETERS
inArea1=arcpy.GetParameterAsText(0) #required study area
lodes=arcpy.GetParameterAsText(1) #requred lodes csv.gz data



###############################################




##---------------------------------------------------------------------------------------------------
# FUNCTIONS
# CREATE FUNCTION TO ADD MESSAGES THROUGHOUT SCRIPT, taken from arcpy documentation
def AddMsgAndPrint(msg, severity=0):
    # Adds a Message (in case this is run as a tool)
    # and also prints the message to the screen (standard output)
    print(msg)

    # Split the message on \n first, so that if its multiple lines,
    # a GPMessage will be added for each line
    try:
        for string in msg.split('\n'):
            # Add appropriate geoprocessing message
            if severity == 0:
                arcpy.AddMessage(string)
            elif severity == 1:
                arcpy.AddWarning(string)
            elif severity == 2:
                arcpy.AddError(string)
    except:
        pass

#CREATE FUNCTION TO BUILD WHERE CLAUSE FROM LIST
def buildWhereClauseFromList(table, field, valueList):
    """Takes a list of values and constructs a SQL WHERE
    clause to select those values within a given field and table."""

    # Add DBMS-specific field delimiters
    fieldDelimited = arcpy.AddFieldDelimiters(arcpy.Describe(table).path, field)

    # Determine field type
    fieldType = arcpy.ListFields(table, field)[0].type

    # Add single-quotes for string field values
    if str(fieldType) == 'String':
        valueList = ["'%s'" % value for value in valueList]

    # Format WHERE clause in the form of an IN statement
    whereClause = "%s IN(%s)" % (fieldDelimited, ', '.join(map(str, valueList)))
    return whereClause



##---------------------------------------------------------------------------------------------------
## 2 USER INPUT STUDY AREA

#check input for type polygon-stolen from arcpy documentation
inArea1Desc=arcpy.Describe(inArea1)
if inArea1Desc.shapeType != "Polygon" :
    AddMsgAndPrint("Input Study Area must be of type polygon.", 2)
    sys.exit()


##---------------------------------------------------------------------------------------------------
## 3 ADD CENSUS DATA TO MAP

# add the living atlas layer from path, with method on map object
arcpy.SetProgressor("","Adding census blocks")
m.addDataFromPath(dataPath)

# get newly added layer as cenBlocks variable
# Note, the layer imports as a group layer, and the sublayer needed is called 'Blocks', which is
# generic and it seems inconsistent, somtimes called 'USA_BLOCK_GROUPS//Blocks, therefore, will
# get the layer into a variable by counting the number of features for every layer in the map,
# if is feature layer and if count==668757, it's the layer we want.
for maplayer in m.listLayers():
    if maplayer.isFeatureLayer:
        if str(arcpy.management.GetCount(maplayer)) == '668757': #IF USING FULL U.S. CENSUS BLOCK SHAPEFILE, CHANGE THIS TO 8180866
            cenBlocks = maplayer


AddMsgAndPrint("Finished adding census blocks", 0)

arcpy.AddMessage(arcpy.management.GetCount(cenBlocks))
#arcpy.management.CopyFeatures(cenBlocksOnline,"cenBlocks")



##---------------------------------------------------------------------------------------------------
## 4 EXTRACT LIST OF HOME CENSUS BLOCKS IN STUDY AREA

arcpy.SetProgressor("","Selecting by location")
AddMsgAndPrint("Selecting by by location", 0)
arcpy.management.SelectLayerByLocation(cenBlocks, 'INTERSECT', inArea1,"",'NEW_SELECTION')
arcpy.AddMessage(arcpy.management.GetCount(cenBlocks))

arcpy.SetProgressor("","Creating census block list")
AddMsgAndPrint("Creating census block list", 0)
blockList = []
with arcpy.da.SearchCursor(cenBlocks, 'GEOID') as cur1:
    for row in cur1:
        blockList.append((int(cur1[0]))) #For some reason, GEOID returns a tuple with an empty second, need to use index 0 to get the id


AddMsgAndPrint(len(blockList))





##---------------------------------------------------------------------------------------------------
## 5 EXTRACT DATA FROM LODES.CSV, FILTER BY STUDY AREA LIST, CREATE REQUIRED DBFS

#Extract LODES csv to Pandas dataframe
AddMsgAndPrint("Extracting LODES csv",0)
df=pd.read_csv(lodes, compression='gzip')
df4=df #to be used later


#Extract where h_geo in SA for resident's work locations inside and outside SA, will split between inside outside later
AddMsgAndPrint("Creating LODES dbf",0)
df=df.query('h_geocode in @blockList') #filter the dataframe by home location corresponding to the list of census blocks in the study area
arcpy.AddMessage(df.shape)
df.to_csv(outcsv)
outdbf=arcpy.conversion.ExportTable(outcsv,"outdbf")

#Next extract where h_geo in SA, w_geo outside SA for home locations of residents who leave SA for work
AddMsgAndPrint("Creating LODES2 dbf",0)
df2=df.query('w_geocode not in @blockList')
arcpy.AddMessage(df2.shape)
df2.to_csv(outcsv2)
outdbf2=arcpy.conversion.ExportTable(outcsv2,"outdbf2")

#Next, extract where h_geo in SA, w_geo inside SA for home locations of residents who stay in SA for work
AddMsgAndPrint("Creating LODES3 dbf",0)
df3=df.query('w_geocode in @blockList')
arcpy.AddMessage(df3.shape)
df3.to_csv(outcsv3)
outdbf3=arcpy.conversion.ExportTable(outcsv3,"outdbf3")

#Finally, extract where w_geo is in SA but h_geo is not for home and work locations of non-residents traveling to the city for work
arcpy.AddMessage(df4.shape)
df4=df4.query('w_geocode in @blockList & h_geocode not in @blockList') #filter the dataframe by work location in city but home location outside of city
arcpy.AddMessage(df4.shape)
df4.to_csv(outcsv4)
outdbf4=arcpy.conversion.ExportTable(outcsv4,"outdbf4")



##---------------------------------------------------------------------------------------------------
## 6 SUM S000 AND GROUP BY TO GET PEOPLE WORKING AND LIVING IN EACH RELATED CENSUS BLOCK
#This works because the data is already filter accordingly from step #, this will show
#how many of the target residents work and live in each census block

#Sum and group by WORK geocode to get work locations for residents, will split by inside/outside SA in join below
AddMsgAndPrint("Creating Sum and Group by Table1",0)
arcpy.analysis.Statistics(outdbf, 'statTable1', [["S000", "SUM"]], "w_geocode") #using result object from ExportTable as input table here
arcpy.management.AddField('statTable1','w_geo_txt', 'TEXT') #w_geocode is a double and it needs to be text to join properly with census blocks, add field and calculate, use str(int(double)) to get text without "xxx.0"
arcpy.management.CalculateField('statTable1','w_geo_txt','str(int(!w_geocode!))') #because both add field and calculte field edit a table rather than generating a new table, don't need to use result object or export fields

#Sum and group by HOME geocode to get home locations from dbf2, which is resident workers leaving the SA for work.
AddMsgAndPrint("Creating Sum and Group by Table2",0)
arcpy.analysis.Statistics(outdbf2,'statTable2',[["S000",'SUM']], "h_geocode") #group by h_geocode to get worker count per home geocode, i.e. the number of people leaving SA for work by census block
arcpy.management.AddField('statTable2','h_geo_txt', 'TEXT')
arcpy.management.CalculateField('statTable2','h_geo_txt','str(int(!h_geocode!))')

#Sum and group by HOME geocode to get home locations from dbf3, which is resident workers staying in the SA for work.
AddMsgAndPrint("Creating Sum and Group by Table3",0)
arcpy.analysis.Statistics(outdbf3,'statTable3',[["S000",'SUM']], "h_geocode")#group by h_geocode to get worker count per home geocode, i.e. the number of people staying in SA for work by census block
arcpy.management.AddField('statTable3','h_geo_txt', 'TEXT')
arcpy.management.CalculateField('statTable3','h_geo_txt','str(int(!h_geocode!))')

#Sum and group by HOME geocode to get home locations from dbf4, which is non-resident workers.
AddMsgAndPrint("Creating Sum and Group by Table4",0)
arcpy.analysis.Statistics(outdbf4,'statTable4',[["S000",'SUM']], "h_geocode")#group by h_geocode to get worker count per home geocode for the people coming into the city for work.
arcpy.management.AddField('statTable4','h_geo_txt', 'TEXT')
arcpy.management.CalculateField('statTable4','h_geo_txt','str(int(!h_geocode!))')

#Sum and group by WORK geocode to get work locations from dbf4, which is non-resident workers.
AddMsgAndPrint("Creating Sum and Group by Table5",0)
arcpy.analysis.Statistics(outdbf4,'statTable5',[["S000",'SUM']], "w_geocode")#group by w_geocode to get worker count per work geocode for the people coming into the city for work.
arcpy.management.AddField('statTable5','w_geo_txt', 'TEXT')
arcpy.management.CalculateField('statTable5','w_geo_txt','str(int(!w_geocode!))')

#---------------------------------------------------------------------------------------------------
# 7 JOIN SUM AND GROUP BY TABLES TO CENSUS BLOCKS, EXTRACT WHERE MATCHING

#join stat table 1 to census blocks intersecting study area = work locations for live in SA and work in SA
AddMsgAndPrint("Creating residentWorkLocationsInsideStudyArea shapefile - this is a long process",0)
arcpy.management.SelectLayerByAttribute(cenBlocks,'CLEAR_SELECTION') #first clear the selection
arcpy.management.SelectLayerByLocation(cenBlocks, 'INTERSECT', inArea1,"",'NEW_SELECTION') #next select cenBlocks INSIDE study area, can be replaced with select by attribute with qry, cenblocls in blockList
resWorkLoc_join=arcpy.management.AddJoin(cenBlocks,'GEOID','statTable1','w_geo_txt','KEEP_COMMON','INDEX_JOIN_FIELDS')
arcpy.management.CopyFeatures(resWorkLoc_join, "residentWorkLocationsInSA")
arcpy.management.RemoveJoin(cenBlocks) #remove the join

#join stat table 1 to census blocks NOT intersecting study area = work locations for live in SA, work outside SA
AddMsgAndPrint("Creating residentWorkLocationsOutsideStudyArea shapefile - this is a long process",0)
arcpy.management.SelectLayerByAttribute(cenBlocks,'CLEAR_SELECTION') #first clear the selection
arcpy.management.SelectLayerByLocation(cenBlocks, 'INTERSECT', inArea1,"",'NEW_SELECTION','INVERT') #next select cenBlocks OUTSIDE study area, can be replaced with select by attribute with qry, cenblocls in blockList
resWorkLoc_join=arcpy.management.AddJoin(cenBlocks,'GEOID','statTable1','w_geo_txt','KEEP_COMMON','INDEX_JOIN_FIELDS') #redo the join with the inverted selection
arcpy.management.CopyFeatures(resWorkLoc_join, "residentWorkLocationsOutsideSA")
arcpy.management.RemoveJoin(cenBlocks)

#join stat table 2 to census blocks=HOME locations for live in SA work outside SA
AddMsgAndPrint("Creating HomeLocationsResidentsWorkOutsideStudyArea shapefile - this is a long process",0)
arcpy.management.SelectLayerByAttribute(cenBlocks,'CLEAR_SELECTION') #first clear the selection on cenBlocks
qry=buildWhereClauseFromList(cenBlocks,'GEOID',blockList) #build a query for the select by attribute
arcpy.management.SelectLayerByAttribute(cenBlocks,'NEW_SELECTION',qry) #select census blocks by list of cenblocks intersecting SA before join to save time, select by attribute is faster than joining to 8 million census blocks
resHomeLocWorkOutside_join=arcpy.management.AddJoin(cenBlocks,'GEOID','statTable2','h_geo_txt','KEEP_COMMON','INDEX_JOIN_FIELDS') #use the KEEP COMMON parameter to 'filter' the join
arcpy.management.CopyFeatures(resHomeLocWorkOutside_join, "residentHomeLocWorkOutsideSA")
arcpy.management.RemoveJoin(cenBlocks)

#join stat table 3 to census blocks=HOME locations for live in SA work inside SA
AddMsgAndPrint("Creating HomeLocationsResidentsWorkInsideStudyArea shapefile - this is a long process",0)
arcpy.management.SelectLayerByAttribute(cenBlocks,'CLEAR_SELECTION')
qry=buildWhereClauseFromList(cenBlocks,'GEOID',blockList)
arcpy.management.SelectLayerByAttribute(cenBlocks,'NEW_SELECTION',qry)
resHomeLocWorkInside_join=arcpy.management.AddJoin(cenBlocks,'GEOID','statTable3','h_geo_txt','KEEP_COMMON','INDEX_JOIN_FIELDS') #use the KEEP COMMON parameter to 'filter' the join
arcpy.management.CopyFeatures(resHomeLocWorkInside_join, "residentHomeLocWorkInsideSA")
arcpy.management.RemoveJoin(cenBlocks)

#join stat table 4 to census blocks=HOME locations for non-resident workers
AddMsgAndPrint("Creating nonResidentHomeLocations shapefile - this is a long process",0)
arcpy.management.SelectLayerByAttribute(cenBlocks,'CLEAR_SELECTION')
#qry=buildWhereClauseFromList(cenBlocks,'GEOID',blockList) #home locations for non-residents will be outside SA, cant select by attribute to speed the join
#arcpy.management.SelectLayerByAttribute(cenBlocks,'NEW_SELECTION',qry)
nonResHomeLoc_join=arcpy.management.AddJoin(cenBlocks,'GEOID','statTable4','h_geo_txt','KEEP_COMMON','INDEX_JOIN_FIELDS') #use the KEEP COMMON parameter to 'filter' the join
arcpy.management.CopyFeatures(nonResHomeLoc_join, "nonResidentHomeLocations")
arcpy.management.RemoveJoin(cenBlocks)

#join stat table 5 to census blocks=WORK locations for non-resident workers
AddMsgAndPrint("Creating nonResidentWorkLocations  shapefile - this is a long process",0)
arcpy.management.SelectLayerByAttribute(cenBlocks,'CLEAR_SELECTION')
qry=buildWhereClauseFromList(cenBlocks,'GEOID',blockList)
arcpy.management.SelectLayerByAttribute(cenBlocks,'NEW_SELECTION',qry)
nonResWorkLoc_join=arcpy.management.AddJoin(cenBlocks,'GEOID','statTable5','w_geo_txt','KEEP_COMMON','INDEX_JOIN_FIELDS') #use the KEEP COMMON parameter to 'filter' the join
arcpy.management.CopyFeatures(nonResWorkLoc_join, "nonResidentWorkLocations")
arcpy.management.RemoveJoin(cenBlocks)

AddMsgAndPrint("Done Creating workLocationsNonResidents shapefile - this is a long process",0)


#---------------------------------------------------------------------------------------------------
# 8 ADD DATA AND SYMBOLIZE

m.addDataFromPath(os.path.join(defaultGDB,"residentWorkLocationsInSA"))
m.addDataFromPath(os.path.join(defaultGDB,"residentWorkLocationsOutsideSA"))
m.addDataFromPath(os.path.join(defaultGDB,"residentHomeLocWorkInsideSA"))
m.addDataFromPath(os.path.join(defaultGDB,"residentHomeLocWorkOutsideSA"))
m.addDataFromPath(os.path.join(defaultGDB,"nonResidentHomeLocations"))
m.addDataFromPath(os.path.join(defaultGDB,"nonResidentWorkLocations"))


l = m.listLayers("nonResidentWorkLocations")[0]
sym = l.symbology
sym.updateRenderer('GraduatedSymbolsRenderer')
if hasattr(sym, 'renderer'):
  if sym.renderer.type == "GraduatedSymbolsRenderer":
    #set background symbol
    sym.renderer.backgroundSymbol.applySymbolFromGallery("Extent Gray Hollow")
    #set symbol template - taken straight from documentation but does not work
    # symTemp = sym.renderer.symbolTemplate
    # symTemp.applySymbolFromGallery('Square 1')
    # sym.renderer.updateSymbolTemplate(symTemp)
    #modify graduated symbol renderer
    sym.renderer.classificationField = "statTable5_SUM_S000"
    sym.classificationMethod = "NaturalBreaks"
    sym.renderer.breakCount = 5
    sym.renderer.minimumSymbolSize = 4
    sym.renderer.maximumSymbolSize = 18
    #sym.renderer.colorRamp = aprx.listColorRamps("Black to White")[0]
    l.symbology = sym

l = m.listLayers("nonResidentHomeLocations")[0]
sym = l.symbology
sym.updateRenderer('GraduatedSymbolsRenderer')
if hasattr(sym, 'renderer'):
  if sym.renderer.type == "GraduatedSymbolsRenderer":
    #set background symbol
    sym.renderer.backgroundSymbol.applySymbolFromGallery("Extent Gray Hollow")
    #set symbol template - taken straight from documentation but does not work
    # symTemp = sym.renderer.symbolTemplate
    # symTemp.applySymbolFromGallery('Square 1')
    # sym.renderer.updateSymbolTemplate(symTemp)
    #modify graduated symbol renderer
    sym.renderer.classificationField = "statTable4_SUM_S000"
    sym.classificationMethod = "NaturalBreaks"
    sym.renderer.breakCount = 5
    sym.renderer.minimumSymbolSize = 4
    sym.renderer.maximumSymbolSize = 18
    #sym.renderer.colorRamp = aprx.listColorRamps("Black to White")[0]
    l.symbology = sym

l = m.listLayers("residentHomeLocWorkOutsideSA")[0]
sym = l.symbology
sym.updateRenderer('GraduatedSymbolsRenderer')
if hasattr(sym, 'renderer'):
    if sym.renderer.type == "GraduatedSymbolsRenderer":
        # set background symbol
        sym.renderer.backgroundSymbol.applySymbolFromGallery("Extent Gray Hollow")
        # set symbol template - taken straight from documentation but does not work
        # symTemp = sym.renderer.symbolTemplate
        # symTemp.applySymbolFromGallery('Square 1')
        # sym.renderer.updateSymbolTemplate(symTemp)
        # modify graduated symbol renderer
        sym.renderer.classificationField = "statTable2_SUM_S000"
        sym.classificationMethod = "NaturalBreaks"
        sym.renderer.breakCount = 5
        sym.renderer.minimumSymbolSize = 4
        sym.renderer.maximumSymbolSize = 18
        # sym.renderer.colorRamp = aprx.listColorRamps("Black to White")[0]
        l.symbology = sym

l = m.listLayers("residentHomeLocWorkInsideSA")[0]
sym = l.symbology
sym.updateRenderer('GraduatedSymbolsRenderer')
if hasattr(sym, 'renderer'):
    if sym.renderer.type == "GraduatedSymbolsRenderer":
        # set background symbol
        sym.renderer.backgroundSymbol.applySymbolFromGallery("Extent Gray Hollow")
        # set symbol template - taken straight from documentation but does not work
        # symTemp = sym.renderer.symbolTemplate
        # symTemp.applySymbolFromGallery('Square 1')
        # sym.renderer.updateSymbolTemplate(symTemp)
        # modify graduated symbol renderer
        sym.renderer.classificationField = "statTable3_SUM_S000"
        sym.classificationMethod = "NaturalBreaks"
        sym.renderer.breakCount = 5
        sym.renderer.minimumSymbolSize = 4
        sym.renderer.maximumSymbolSize = 18
        # sym.renderer.colorRamp = aprx.listColorRamps("Black to White")[0]
        l.symbology = sym

l = m.listLayers("residentWorkLocationsOutsideSA")[0]
sym = l.symbology
sym.updateRenderer('GraduatedSymbolsRenderer')
if hasattr(sym, 'renderer'):
    if sym.renderer.type == "GraduatedSymbolsRenderer":
        # set background symbol
        sym.renderer.backgroundSymbol.applySymbolFromGallery("Extent Gray Hollow")
        # set symbol template - taken straight from documentation but does not work
        # symTemp = sym.renderer.symbolTemplate
        # symTemp.applySymbolFromGallery('Square 1')
        # sym.renderer.updateSymbolTemplate(symTemp)
        # modify graduated symbol renderer
        sym.renderer.classificationField = "statTable1_SUM_S000"
        sym.classificationMethod = "NaturalBreaks"
        sym.renderer.breakCount = 5
        sym.renderer.minimumSymbolSize = 4
        sym.renderer.maximumSymbolSize = 18
        # sym.renderer.colorRamp = aprx.listColorRamps("Black to White")[0]
        l.symbology = sym

l = m.listLayers("residentWorkLocationsInSA")[0]
sym = l.symbology
sym.updateRenderer('GraduatedSymbolsRenderer')
if hasattr(sym, 'renderer'):
    if sym.renderer.type == "GraduatedSymbolsRenderer":
        # set background symbol
        sym.renderer.backgroundSymbol.applySymbolFromGallery("Extent Gray Hollow")
        # set symbol template - taken straight from documentation but does not work
        # symTemp = sym.renderer.symbolTemplate
        # symTemp.applySymbolFromGallery('Square 1')
        # sym.renderer.updateSymbolTemplate(symTemp)
        # modify graduated symbol renderer
        sym.renderer.classificationField = "statTable1_SUM_S000"
        sym.classificationMethod = "NaturalBreaks"
        sym.renderer.breakCount = 5
        sym.renderer.minimumSymbolSize = 4
        sym.renderer.maximumSymbolSize = 18
        # sym.renderer.colorRamp = aprx.listColorRamps("Black to White")[0]
        l.symbology = sym

arcpy.AddMessage('end')


