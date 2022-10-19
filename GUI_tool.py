import tkinter as tk
from tkinter import Label, Button, Entry
from string_matching import *
from tkinter.filedialog import askopenfilename
from tkinter.filedialog import asksaveasfilename
from tkinter.messagebox import showerror
import time


options_countrycodes = ('Country Name','Isocode2','Isocode3')
options_distance = ('Levenshtein', 'Damerau Levenshtein')
y_att_position = 180

class MyWindow:
    def __init__(self, win):

        self.buttonBrowse = Button(win, text='Load First Dataset',command=self.import_csv1)
        self.buttonBrowse.place(x=55, y=30)
        self.lbl1 = Entry(width=50)
        self.lbl1.place(x=250, y=35)

        self.buttonBrowse2 = Button(win, text='Load Second Dataset',command=self.import_csv2)
        self.buttonBrowse2.place(x=50, y=80)
        self.lbl2 = Entry(width=50)
        self.lbl2.place(x=250, y=85)

    # List of Attributes
        self.idL = tk.StringVar()
        self.idattr = Label(win, textvariable=self.idL, borderwidth=0)
        self.idattr.place(x=55, y = y_att_position)
        self.idL.set("Identifier*")

        self.nameL = tk.StringVar()
        self.nameAtt = Label(win, textvariable=self.nameL, borderwidth=0)
        self.nameAtt.place(x=55, y = y_att_position+40)
        self.nameL.set("Entity names*")

        self.cntL = tk.StringVar()
        self.cntAtt = Label(win, textvariable=self.cntL, borderwidth=0)
        self.cntAtt.place(x=55, y = y_att_position+40*2)
        self.cntL.set("Country*")

        self.typeCL = tk.StringVar()
        self.tcAtt = Label(win, textvariable=self.typeCL, borderwidth=0)
        self.tcAtt.place(x=55, y = y_att_position+40*3)
        self.typeCL.set("Type of country code*")

        self.ctyL = tk.StringVar()
        self.ctyAtt = Label(win, textvariable=self.ctyL, borderwidth=0)
        self.ctyAtt.place(x=55, y = y_att_position+40*4)
        self.ctyL.set("City")

        self.postL = tk.StringVar()
        self.pstAtt = Label(win, textvariable=self.postL, borderwidth=0)
        self.pstAtt.place(x=55, y = y_att_position+40*5)
        self.postL.set("Postal Code")

        self.addL = tk.StringVar()
        self.addAtt = Label(win, textvariable=self.addL, borderwidth=0)
        self.addAtt.place(x=55, y = y_att_position+40*6)
        self.addL.set("Addresses")


    # Two columns for attributes choice
        self.co1 = tk.StringVar()
        self.attributesData1 = Label(win, textvariable=self.co1,  borderwidth=0)
        self.attributesData1.place(x=270, y = 130)
        self.co1.set("First Dataset")

        self.co2 = tk.StringVar()
        self.attributesData2 = Label(win, textvariable=self.co2,  borderwidth=0)
        self.attributesData2.place(x=440, y = 130)
        self.co2.set("Second Dataset")

     # Attributes first dataset
        self.choices_ID = tk.StringVar(win)
        self.choices_ID.set('NA') 
        self.wID1 = tk.OptionMenu(win, self.choices_ID, 'NA')
        self.wID1.place(x=275, y= y_att_position-10)

        self.choices_name = tk.StringVar(win)
        self.choices_name.set('NA') 
        self.wNM = tk.OptionMenu(win, self.choices_name, 'NA')
        self.wNM.place(x=275, y= y_att_position+40-10)

        self.choices_cnt = tk.StringVar(win)
        self.choices_cnt.set('NA') 
        self.wC = tk.OptionMenu(win, self.choices_cnt, 'NA')
        self.wC.place(x=275, y= y_att_position+40*2-10)

        self.countrycode1 = tk.StringVar(win)
        self.countrycode1.set('NA') 
        self.wCC = tk.OptionMenu(win, self.countrycode1, *options_countrycodes)
        self.wCC.place(x=275, y= y_att_position+40*3-10)

        self.choices_cty = tk.StringVar(win)
        self.choices_cty.set('NA') 
        self.wCT = tk.OptionMenu(win, self.choices_cty, 'NA')
        self.wCT.place(x=275, y= y_att_position+40*4-10)

        self.choices_pc = tk.StringVar(win)
        self.choices_pc.set('NA') 
        self.wP = tk.OptionMenu(win, self.choices_pc, 'NA')
        self.wP.place(x=275, y= y_att_position+40*5-10)

        self.choices_add = tk.StringVar(win)
        self.choices_add.set('NA') 
        self.wAD = tk.OptionMenu(win, self.choices_add, 'NA')
        self.wAD.place(x=275, y= y_att_position+40*6-10)

    # Attributes second dataset
        self.choices_ID2 = tk.StringVar(win)
        self.choices_ID2.set('NA') 
        self.wID2 = tk.OptionMenu(win, self.choices_ID2, 'NA')
        self.wID2.place(x=445, y= y_att_position-10)

        self.choices_name2 = tk.StringVar(win)
        self.choices_name2.set('NA') 
        self.wNM2 = tk.OptionMenu(win, self.choices_name2, 'NA')
        self.wNM2.place(x=445, y= y_att_position+40-10)

        self.choices_cnt2 = tk.StringVar(win)
        self.choices_cnt2.set('NA') 
        self.wC2 = tk.OptionMenu(win, self.choices_cnt2, 'NA')
        self.wC2.place(x=445, y= y_att_position+40*2-10)

        self.countrycode2 = tk.StringVar(win)
        self.countrycode2.set('NA') 
        self.wCC2 = tk.OptionMenu(win, self.countrycode2, *options_countrycodes)
        self.wCC2.place(x=445, y= y_att_position+40*3-10)

        self.choices_cty2 = tk.StringVar(win)
        self.choices_cty2.set('NA') 
        self.wCT2 = tk.OptionMenu(win, self.choices_cty2, 'NA')
        self.wCT2.place(x=445, y= y_att_position+40*4-10)

        self.choices_pc2 = tk.StringVar(win)
        self.choices_pc2.set('NA') 
        self.wP2 = tk.OptionMenu(win, self.choices_pc2, 'NA')
        self.wP2.place(x=445, y= y_att_position+40*5-10)

        self.choices_add2 = tk.StringVar(win)
        self.choices_add2.set('NA') 
        self.wAD2 = tk.OptionMenu(win, self.choices_add2, 'NA')
        self.wAD2.place(x=445, y= y_att_position+40*6-10)

    # Load dictionaries
        self.buttonDictOff = Button(win, text='Load Abbreviation Dictionary',command=self.import_dictionary)
        self.buttonDictOff.place(x=80, y=470)
        self.messageDict1 = ""
        self.textDict1= tk.StringVar()
        self.textDict1.set(self.messageDict1)
        self.label_Dict1 = Label(win, textvariable=self.textDict1)
        self.label_Dict1.place(x=260, y = 472)
        self.tickVar = tk.IntVar()
        self.tickDict1 = tk.Checkbutton(win, text='Use Dictionary',variable=self.tickVar, onvalue=1, offvalue=0)
        self.tickDict1.place(x=350, y=470)

        self.buttonDictFuz = Button(win, text='Load Fuzzy Dictionary',command=self.import_dictionary2)
        self.buttonDictFuz.place(x=95, y=510)
        self.messageDict2 = ""
        self.textDict2= tk.StringVar()
        self.textDict2.set(self.messageDict2)
        self.label_Dict2 = Label(win, textvariable=self.textDict2)
        self.label_Dict2.place(x=260, y = 512)
        self.tickVar2 = tk.IntVar()
        self.tickDict2 = tk.Checkbutton(win, text='Use Dictionary',variable=self.tickVar2, onvalue=1, offvalue=0)
        self.tickDict2.place(x=350, y=510)

        self.buttonIsocode = Button(win, text='Load Country Name-Isocode Converter',command=self.import_isocodes)
        self.buttonIsocode.place(x=55, y=550)
        self.messageIS = ""
        self.textIS= tk.StringVar()
        self.textIS.set(self.messageIS)
        self.label_IS = Label(win, textvariable=self.textIS)
        self.label_IS.place(x=350, y = 555)

    # Tool Additional Options
        self.textOption = tk.StringVar()
        self.textOption.set('Additional Options')
        self.labelOptions = Label(win, textvariable=self.textOption)
        self.labelOptions.place(x=880, y = 20)

        # AI Parser addresses
        self.tickAIPars = tk.BooleanVar()
        self.tickAIPB = tk.Checkbutton(win, text='Use AI to parse the address: in case of big datasets this option significantly \n\
        slows down the performance. If unticked, addresses are parsed too \nbut arbitrary methods will be used',\
            variable=self.tickAIPars, onvalue=True, offvalue=False)
        self.tickAIPB.place(x=700, y=60)

        # Perfect match addresses
        self.tickPM = tk.BooleanVar()
        self.tickPMB = tk.Checkbutton(win, text='Perfect Match on addresses if ticked (no similarity used).',\
            variable=self.tickPM, onvalue=True, offvalue=False)
        self.tickPMB.place(x=700, y=125)

        # Fuzzy Distance
        self.textFD = tk.StringVar()
        self.textFD.set('Choose Similarity Metric for addresses:')
        self.labelFD = Label(win, textvariable=self.textFD)
        self.labelFD.place(x=710, y = 160)

        self.fuzzyDist = tk.StringVar(win)
        self.fuzzyDist.set(options_distance[0]) 
        self.wFD = tk.OptionMenu(win, self.fuzzyDist, *options_distance)
        self.wFD.place(x=940, y= 155)

        self.textTolerance = tk.StringVar()
        self.textTolerance.set('Tolerance Level Ratio [0-1]:')
        self.labelTol = Label(win, textvariable=self.textTolerance)
        self.labelTol.place(x=710, y = 190)

        self.lblTR = Entry(width=10)
        self.lblTR.place(x=950, y=190)
        self.lblTR.insert(0,'0.8')

        # Notes
        self.textNotes = tk.StringVar()
        self.textNotes.set('Notes:\n\
        - Attributes with * are the minimum requirements needed to run the tool\n\
        - Use this tool for small datasets: in case of big data, please consider\n\
            switching to the PySpark version.\n\
        - The higher the tolerance level ratio chosen, the higher the precision\n\
        - In case an attribute is not available, choose NA')
        self.labelNO = Label(win, textvariable=self.textNotes, justify='left')
        self.labelNO.place(x=700, y = 240)

    # Matching
        self.matchingB = Button(win, text='Run the tool!',command=self.matching, height=2, width=40, borderwidth=3)
        self.matchingB.place(x=790, y=340)

        self.messageRun = ""
        self.textRun = tk.StringVar()
        self.textRun.set(self.messageRun)
        self.labelRun = Label(win, textvariable=self.textRun, justify='left')
        self.labelRun.place(x=730, y = 400)

    # Summary results
        self.textSum = tk.StringVar()
        self.textSum.set('Preliminary analysis of results:\n')
        self.labelSU = Label(win, textvariable=self.textSum, justify='left')
        self.labelSU.place(x=700, y = 550)

    # Save results
        self.message = ""
        self.label_text = tk.StringVar()
        self.label_text.set(self.message)
        self.label_save = Label(win, textvariable=self.label_text)
        self.label_save.place(x=980, y = 635)

        self.buttonExport = Button(win, text='Save Results',command=self.save_file_xlsx)
        self.buttonExport.place(x=900, y=630)
    

    def import_csv1(self):
        self.lbl1.delete(0, 'end')
        csv_file_path = askopenfilename()
        global df1
        try:
            df1 = pd.read_csv(csv_file_path, sep=',')
        except:
            try:
                df1 = pd.read_csv(csv_file_path, sep=';')
            except:
                df1 = pd.DataFrame()

        if len(df1)>0:
            self.lbl1.insert(0, 'Dataset successfully loaded. Number of records: '+str(len(df1)))
            columns_data1 = df1.columns.to_list()
            columns_data1.append('NA')
            self.choices_ID.set('NA')
            self.choices_name.set('NA')
            self.choices_cnt.set('NA')
            self.choices_add.set('NA')
            self.choices_cty.set('NA')
            self.choices_pc.set('NA')
            self.wID1['menu'].delete(0, 'end')
            self.wNM['menu'].delete(0, 'end')
            self.wC['menu'].delete(0, 'end')
            self.wAD['menu'].delete(0, 'end')
            self.wCT['menu'].delete(0, 'end')
            self.wP['menu'].delete(0, 'end')
            for choice in columns_data1:
                self.wID1['menu'].add_command(label=choice, command=tk._setit(self.choices_ID, choice))
                self.wNM['menu'].add_command(label=choice, command=tk._setit(self.choices_name, choice))
                self.wC['menu'].add_command(label=choice, command=tk._setit(self.choices_cnt, choice))
                self.wAD['menu'].add_command(label=choice, command=tk._setit(self.choices_add, choice))
                self.wCT['menu'].add_command(label=choice, command=tk._setit(self.choices_cty, choice))
                self.wP['menu'].add_command(label=choice, command=tk._setit(self.choices_pc, choice))
        else:
            self.lbl1.insert(0, 'Dataset loading failed. Please select another csv')

    def import_csv2(self):
        self.lbl2.delete(0, 'end')
        csv_file_path = askopenfilename()
        global df2
        try:
            df2 = pd.read_csv(csv_file_path, sep=',')
        except:
            try:
                df2 = pd.read_csv(csv_file_path, sep=';')
            except:
                df2 = pd.DataFrame()

        if len(df2)>0:
            self.lbl2.insert(0, 'Dataset successfully loaded. Number of records: '+str(len(df2)))
            columns_data2 = df2.columns.to_list()
            columns_data2.append('NA')
            self.choices_ID2.set('NA')
            self.choices_name2.set('NA')
            self.choices_cnt2.set('NA')
            self.choices_add2.set('NA')
            self.choices_cty2.set('NA')
            self.choices_pc2.set('NA')
            self.wID2['menu'].delete(0, 'end')
            self.wNM2['menu'].delete(0, 'end')
            self.wC2['menu'].delete(0, 'end')
            self.wAD2['menu'].delete(0, 'end')
            self.wCT2['menu'].delete(0, 'end')
            self.wP2['menu'].delete(0, 'end')
            for choice in columns_data2:
                self.wID2['menu'].add_command(label=choice, command=tk._setit(self.choices_ID2, choice))
                self.wNM2['menu'].add_command(label=choice, command=tk._setit(self.choices_name2, choice))
                self.wC2['menu'].add_command(label=choice, command=tk._setit(self.choices_cnt2, choice))
                self.wAD2['menu'].add_command(label=choice, command=tk._setit(self.choices_add2, choice))
                self.wCT2['menu'].add_command(label=choice, command=tk._setit(self.choices_cty2, choice))
                self.wP2['menu'].add_command(label=choice, command=tk._setit(self.choices_pc2, choice))
 
        else:
            self.lbl2.insert(0, 'Dataset loading failed. Please select another csv')

    def import_dictionary(self):
        self.messageDict1 = 'Loading...'
        self.textDict1.set(self.messageDict1)
        csv_file_path = askopenfilename()
        global abbreviations
        try:
            abbreviations = pd.read_csv(csv_file_path, sep=',')
            abbreviations = removeSpaces(removePunctuation(convert_accent(abbreviations, 'abbr'), 'abbr'), 'abbr')
            abbreviations = removeSpaces(removePunctuation(convert_accent(abbreviations, 'descr'), 'descr'), 'descr')
            self.messageDict1 = 'Done!'
            self.textDict1.set(self.messageDict1)
        except:
            try:
                abbreviations = pd.read_csv(csv_file_path, sep=',')
                abbreviations = removeSpaces(removePunctuation(convert_accent(abbreviations, 'abbr'), 'abbr'), 'abbr')
                abbreviations = removeSpaces(removePunctuation(convert_accent(abbreviations, 'descr'), 'descr'), 'descr')
                self.messageDict1 = 'Done!'
                self.textDict1.set(self.messageDict1)
            except:
                abbreviations = pd.DataFrame()
                self.messageDict1 = 'Error'
                self.textDict1.set(self.messageDict1) 

    def import_dictionary2(self):
        self.messageDict2 = 'Loading...'
        self.textDict2.set(self.messageDict2)
        csv_file_path = askopenfilename()
        global fuzzy_dict
        try:
            fuzzy_dict = pd.read_csv(csv_file_path, sep=',')
            fuzzy_dict[['isocode', 'pattern',	'replacement']] = fuzzy_dict[['isocode', 'pattern',	'replacement']].astype(str)
            fuzzy_dict['replacement'] = fuzzy_dict['replacement'].apply(lambda x: re.sub('nan','',str(x)))
            self.messageDict2 = 'Done!'
            self.textDict2.set(self.messageDict2)
        except:
            try:
                fuzzy_dict = pd.read_csv(csv_file_path, sep=',')
                fuzzy_dict[['isocode', 'pattern',	'replacement']] = fuzzy_dict[['isocode', 'pattern',	'replacement']].astype(str)
                fuzzy_dict['replacement'] = fuzzy_dict['replacement'].apply(lambda x: re.sub('nan','',str(x)))
                self.messageDict2 = 'Done!'
                self.textDict2.set(self.messageDict2)
            except:
                fuzzy_dict = pd.DataFrame()
                self.messageDict2 = 'Error'
                self.textDict2.set(self.messageDict2) 

    def import_isocodes(self):
        self.messageIS = 'Loading...'
        self.textIS.set(self.messageIS)
        csv_file_path = askopenfilename()
        global isocodes
        try:
            isocodes = pd.read_csv(csv_file_path, sep=',')[['name', 'alpha-2', 'alpha-3']]
            isocodes[['name', 'alpha-2', 'alpha-3']] = isocodes[['name', 'alpha-2', 'alpha-3']].astype(str)
            isocodes.rename(columns={'name': 'Country Name','alpha-2':'Isocode2','alpha-3':'Isocode3'}, inplace=True)
            self.messageIS = 'Done!'
            self.textIS.set(self.messageIS)
        except:
            try:
                isocodes = pd.read_csv(csv_file_path, sep=',')[['name', 'alpha-2', 'alpha-3']]
                isocodes[['name', 'alpha-2', 'alpha-3']] = isocodes[['name', 'alpha-2', 'alpha-3']].astype(str)
                isocodes.rename(columns={'name': 'Country Name','alpha-2':'Isocode2','alpha-3':'Isocode3'}, inplace=True)
                self.messageDict2 = 'Done!'
                self.textIS.set(self.messageIS)
            except:
                isocodes = pd.DataFrame()
                self.messageIS = 'Error'
                self.textIS.set(self.messageIS) 


    def matching(self):
        self.messageRun = '-Tool started...\n-Preprocessing First Dataset...'
        self.textRun.set(self.messageRun)
        window.update()
        global df1_clean
        df1_clean = data_preparation(df1,  identifier = self.choices_ID.get(), \
                                     name_entity = self.choices_name.get(), \
                                     address = self.choices_add.get(), \
                                     city = self.choices_cty.get(), \
                                     postal_code = self.choices_pc.get(), \
                                     country = self.choices_cnt.get(), \
                                     AIparser=self.tickAIPars.get())
        self.messageRun = self.messageRun+' Done!\n-Preprocessing Second Dataset...  '
        self.textRun.set(self.messageRun)
        window.update()

        global df2_clean
        df2_clean = data_preparation(df2,  identifier = self.choices_ID2.get(), \
                                     name_entity = self.choices_name2.get(), \
                                     address = self.choices_add2.get(), \
                                     city = self.choices_cty2.get(), \
                                     postal_code = self.choices_pc2.get(), \
                                     country = self.choices_cnt2.get(), \
                                     AIparser=self.tickAIPars.get())

        if self.countrycode1.get() != self.countrycode2.get():
            try:
                self.messageRun = self.messageRun+' Done!\n-Fixing Isocodes..  '
                self.textRun.set(self.messageRun)
                window.update()
                df1_clean = convert_isocode(df1_clean, isocodes, self.countrycode1.get())
                df2_clean = convert_isocode(df2_clean, isocodes, self.countrycode2.get())
            except:
                self.messageRun = self.messageRun+' Done!\n-Fixing Isocodes.. Something went wrong! Check country code input'
                self.textRun.set(self.messageRun)
                window.update()

        if self.tickVar.get() == 1 and len(abbreviations)>0:
            self.messageRun = self.messageRun+' Done!\n-Applying Abbreviation Dictionary...'
            self.textRun.set(self.messageRun)
            window.update()

            df1_clean = apply_dictionary(df1_clean, \
                                        country ='country', \
                                        column_dict = 'name', \
                                        dictionary = abbreviations, \
                                        pattern = 'descr', \
                                        replacement = 'abbr',\
                                        isocode = 'isocode')

            df2_clean = apply_dictionary(df2_clean, \
                                        country ='country', \
                                        column_dict = 'name', \
                                        dictionary = abbreviations, \
                                        pattern = 'descr', \
                                        replacement = 'abbr',\
                                        isocode = 'isocode')
            self.messageRun = self.messageRun+' Done!\n'
            self.textRun.set(self.messageRun)
            window.update()

        if self.tickVar2.get() == 1 and len(fuzzy_dict)>0:
            self.messageRun = self.messageRun+' Done!\n-Applying fuzzy Dictionary...'
            self.textRun.set(self.messageRun)
            window.update()

            df1_clean = apply_dictionary(df1_clean, \
                                        country ='country', \
                                        column_dict = 'name', \
                                        dictionary = fuzzy_dict, \
                                        pattern = 'pattern', \
                                        replacement = 'replacement',\
                                        isocode = 'isocode')

            df2_clean = apply_dictionary(df2_clean, \
                                        country ='country', \
                                        column_dict = 'name', \
                                        dictionary = fuzzy_dict, \
                                        pattern = 'pattern', \
                                        replacement = 'replacement',\
                                        isocode = 'isocode')
            self.messageRun = self.messageRun+' Done!\n'
            self.textRun.set(self.messageRun)
            window.update()
            
        self.messageRun = self.messageRun+'-Performing matching...\n'
        self.textRun.set(self.messageRun)
        window.update()
        
        global matches
        matches = stringMatching(df1_clean, self.choices_ID.get(), df2_clean, self.choices_ID2.get(),\
                                 self.tickPM.get(), self.fuzzyDist.get(), float(self.lblTR.get()), \
                                 self.choices_cty.get(), self.choices_pc.get())
        
        global enriched_matches
        enriched_matches = joinOriginalData(matches, df1, self.choices_ID.get(), df2, \
            self.choices_ID2.get(), self.choices_name.get(), self.choices_name2.get(), \
            self.choices_add.get(), self.choices_add2.get())

        self.messageRun = self.messageRun+"Complete!"
        self.textRun.set(self.messageRun)

        n_obs = len(enriched_matches)
        ID_firstdataset = str(len(enriched_matches[[self.choices_ID.get()]].drop_duplicates()))
        ID_seconddataset = str(len(enriched_matches[[self.choices_ID2.get()]].drop_duplicates()))

        self.textSum.set('Analysis of results:\n\
        - Matches found: '+str(n_obs)+'\n\
        - Unique '+ self.choices_ID.get()+' '+ID_firstdataset+'\n\
        - Unique '+ self.choices_ID2.get()+' '+ID_seconddataset)
    
    def save_file_xlsx(self):
        try:
            self.message = "Browsing..."
            self.label_text.set(self.message)
            savefile = asksaveasfilename(filetypes=(("Excel files", "*.xlsx"),
                                                        ("All files", "*.*") ))
            enriched_matches.to_excel(savefile + ".xlsx", index=False, sheet_name="Results")         
            self.message = "Saved!"
            self.label_text.set(self.message)
        except:
            self.message = "Error"
            self.label_text.set(self.message)
        return

window=tk.Tk()
mywin=MyWindow(window)
window.title('Name Address Matching Tool')
window.geometry("1200x700+10+10")
window.mainloop()


