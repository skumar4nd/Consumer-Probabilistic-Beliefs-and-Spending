#import libaries and data
import pandas as pd
import numpy as np
import scipy.stats as stats

# Import Survey of Consumer Expectations data from NY FED Data Center
url1="https://www.newyorkfed.org/medialibrary/interactives/sce/sce/downloads/data/frbny-sce-public-microdata-complete-13-16.xlsx"
url2="https://www.newyorkfed.org/medialibrary/interactives/sce/sce/downloads/data/frbny-sce-public-microdata-complete-17-19.xlsx"
url3="https://www.newyorkfed.org/medialibrary/Interactives/sce/sce/downloads/data/frbny-sce-public-microdata-latest"
sce13_16= pd.read_excel(url1,skiprows=[0])
sce17_19= pd.read_excel(url2,skiprows=[0])
sce20_21= pd.read_excel(url3,skiprows=[0])

#Check data size, number of rows and columns
#sce13_16.info()
#sce17_19.info()
#sce20_21.info()
# concat data sets to create master file
frames = [sce13_16, sce17_19, sce20_21]
sce= pd.concat(frames,ignore_index=True, sort=False)
sce.info()
#sce[-3:]

#Clean SCE data

#Generate Year and month identifier, rearrane columns 
sce['time'] = pd.to_datetime(sce['date'], format='%Y%m')
fc=sce.pop('userid')
sce.insert(0, 'userid', fc)

fc = sce.pop('time')
sce.insert(2, 'time', fc)

sce.sort_values(by=['userid','time'])
sce.head(25)

#Check reported probabilities sum to zero for inflation, earnings and housing expectations
#sce['Q9_bin1'].info()
col=sce.columns

#Inflation
ind=sce.columns.get_loc("Q9_bin1") 
#print(sce.iloc[1:10, ind:ind+9])
#bs = pd.isnull(sce["Q9_bin1"])
#sce['Q9_bin1'][bs]

#Replace missing probability values for with zero and check sum=100
for i in col[ind:ind+10]:
    sce[i]=sce[i].fillna(0)
c=col[ind:ind+10]    
sce['inf1_p']=sce[c].sum(axis=1)

ind=sce.columns.get_loc("Q9c_bin1")
for i in col[ind:ind+10]:
    sce[i]=sce[i].fillna(0)
c=col[ind:ind+10]    
sce['inf2_p']=sce[c].sum(axis=1)


#Earnings    
ind=sce.columns.get_loc("Q24_bin1")
for i in col[ind:ind+10]:
    sce[i]=sce[i].fillna(0)
c=col[ind:ind+10]    
sce['earn_p']=sce[c].sum(axis=1)

#House Prices  
ind=sce.columns.get_loc("C1_bin1")
for i in col[ind:ind+10]:
    sce[i]=sce[i].fillna(0)    
c=col[ind:ind+10]    
sce['house_p']=sce[c].sum(axis=1)

#Count how many probabilities do not sum to zero
#sce.inf1_p[sce['inf1_p'] ==100.00 ].count() 
#sce.inf2_p[sce['inf2_p'] ==100.00 ].count() 
#sce.earn_p[sce['earn_p'] ==100.00 ].count() 
#sce.house_p[sce['house_p'] ==100.00 ].count() 

#Drop observation if reported probabilities of inflation expectations at 1-year and earnings do not sum to 100
dropif=sce[(sce['inf1_p']<100) | (sce['earn_p']<100) ].index
sce.drop(dropif,inplace=True)
id_tot=len(pd.unique(sce['userid']))
month_tot=len(pd.unique(sce['date']))
#sce.info()

#rename columns and create some columns for socio-economic characteristics
sce.rename(columns={"userid": "id",
                    "Q24_mean":"earn_mean",
                    "Q25v2part2":"hh_income",
                    "Q26v2part2": "hh_spending",
                    "Q9_mean":"inf1y_mean",
                    "Q4new":"unemp_exp",
                    "Q5new":"roi_exp",
                    "Q30new":"default_mean"}, inplace="TRUE")
sce['earn_sd']=np.power((sce['Q24_var']),0.5)

# race
col         = 'Q35_'
conditions  = [ sce[col+'1'] ==1, sce[col+'2'] ==1, sce[col+'3'] ==1, sce[col+'4'] ==1, sce[col+'5'] ==1, sce[col+'6'] ==1, sce['Q34'] ==1]
choices     = [ 'White', 'Black', 'Indian', 'Asian', 'Islander', 'Other race', 'Hispanic/Latino' ] 

sce["race"] = np.select(conditions, choices)
sce["race"]=sce.groupby("id")["race"].transform("first")

#family members in primary house
#col         = 'Q45new_'
#for i in "12345678":
#    sce[col+i]=sce.groupby("id")[col+i].transform("first")

sce["Q43"]=sce.groupby("id")["Q43"].transform("first")
sce["age"]=sce.groupby("id")["Q32"].transform("first")
sce["age2"]=sce['age']*sce['age']
sce["age3"]=sce['age2']*sce['age']
sce["gender"]=sce.groupby("id")["Q33"].transform("first")
sce["married"]=sce.groupby("id")["Q38"].transform("first")
sce["income"]=sce.groupby("id")["Q47"].transform("first")

sce["college"]=np.where(sce['Q36']<5, 0, 1)
sce["howner"]=np.where(sce['Q43']==1, 1, 0)
sce["emp"]=np.where(sce['Q10_1']==1 | Q10_2==1 | Q10_4==1 | Q10_5==1, 1, 0)

#Financial and credit situation expectations
sce["efs"]=np.where(sce['Q2']>=3, 1, 0)
sce["cfs"]=np.where(sce['Q1']>=3, 1, 0)
sce["ccs"]=np.where(sce['Q28']>=3, 1, 0)
sce["ecs"]=np.where(sce['Q29']>=3, 1, 0)

df.rename(columns={"_EDU_CAT": "edu", "_HH_INC_CAT": "inc_c", "_NUM_CAT":"ability",
                   "_COMMUTING_ZONE":"zip","Q13new":"jobloss","Q14new":"jobleave"})
#Earnings
conditions  = [ sce['Q47'] ==1, sce['Q47'] ==2, sce['Q47'] ==3, sce['Q47'] ==4, sce['Q47'] ==5, sce['Q47'] ==6, sce['Q47'] ==7,sce['Q47'] ==8,sce['Q47'] ==9,sce['Q47'] ==10,sce['Q47'] ==11]
choices     = [ '5000','15000','25000','35000','45000','55000','67500','87500','125000','175000','250000' ] 


sce['dm']=sce['default_mean']/100
sce['drisk']=(sce['dm']*(1-sce['dm']))^0.5
sce['gdebt']=np.where(sce['C3part2']<=100 & sce['C3part2']>=-100,C3part2/100,np.nan)
sce['taxg']=sce['Q27v2part2']/100


