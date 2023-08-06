import os
import pandas as pd

def combine_csv_files(folder_path): 
    """
    Combines all the CSV files in the folder and returns a dataframe
    """
    df_csv_append = pd.DataFrame()
    
    # append the CSV files
    for file in os.listdir(folder_path):
        df = pd.read_csv(os.path.join(folder_path, file))
        df_csv_append = df_csv_append.append(df, ignore_index=True)
    
    return df_csv_append

def calculate_cpr_pivots(combined_df):
    """
    Takes a dataframe with High Low Close price, calculates the CPR, pivots and CPR width and returns the list
    of same which needs to be appended as colums in dataframe
    """

    #Initializing the list's first element as 0 as we can caluclate for the first day
    CPR = [0]
    CPRh = [0]
    CPRl = [0]
    R1 = [0]
    R2 = [0]
    R3 = [0]
    S1 = [0]
    S2 = [0]
    S3 = [0]
    CPRWidth = [0]

    for ind in combined_df.index:
        try:
            # CPR, R1, R2, R3, S1, S2, S3 and CPR width calcualtions
            high = int(combined_df['High '][ind])
            low = int(combined_df['Low '][ind])
            close = int(combined_df['Close '][ind])
            iCPR = (high + low + close) / 3
            iCPRl = (high + low) / 2
            iCPRh = (iCPR - iCPRl) +iCPR
            iCPRWidth = ((iCPRh - iCPRl ) / iCPR) * 100 # ((tBC - tTC)/tP)*100
            R1.append((2 * iCPR) - low) # (2 * P) – L
            R2.append(iCPR + (high - low))# P + (H – L)
            R3.append(high + 2 * (iCPR - low))
            S1.append((2 * iCPR) - high)# (2 * P) – H
            S2.append(iCPR -(high - low)) #  P – (H – L)
            S3.append(low - 2 * (high - iCPR))
            CPR.append(iCPR)
            CPRh.append(iCPRh)
            CPRl.append(iCPRl)
           
            CPRWidth.append(abs(iCPRWidth))

        except Exception as e:
            #Mark the values as o if any issue occurs during calculation
            CPR.append(0)
            CPRh.append(0)
            CPRl.append(0)
            R1.append(0)
            R2.append(0)
            R3.append(0)
            S1.append(0)
            S2.append(0)
            S3.append(0)
            CPRWidth.append(0)
    return CPR, CPRh, CPRl, R1, R2, R3, S1, S2, S3, CPRWidth

def calPriceVol(combined_df):
    """
    Logic to find which support or resistance the price as crossed
    Takes dataframe as input and returns list of values that needs to be appended in dataframe
    """
    crossed = []
    for ind in combined_df.index:
        try:
            high = int(combined_df['High '][ind])
            low = int(combined_df['Low '][ind])
            if high > int(combined_df['R3'][ind]):
                icrossed = 'R3'
            elif high > int(combined_df['R2'][ind]):
                icrossed = 'R2'
            elif high > int(combined_df['R1'][ind]):
                icrossed = 'R1'
            else:
                icrossed ='R'
            if low < int(combined_df['S3'][ind]):
                icrossed = icrossed + 'S3'
            elif low < int(combined_df['S2'][ind]):
                icrossed = icrossed + 'S2'
            elif low < int(combined_df['S1'][ind]):
                icrossed = icrossed + 'S1'
            else:
                icrossed =icrossed + 'S'
            crossed.append(icrossed)
        except Exception as e:
            crossed.append('-')
    return crossed

def calProbofNarrowCPR(combined_df):
    """
    Logic to find if the trade worked as per CPR rules and mark is as 'Y' or 'N' in the worked column
    """
    worked = []

    for ind in combined_df.index:
        try:
            if ('2' in combined_df['crossed'][ind] or '3' in combined_df['crossed'][ind]) and (float(combined_df['CPRWidth'][ind]) <= 0.20):
                worked.append('Y')
            elif ('1' in combined_df['crossed'][ind] or 'RS' in combined_df['crossed'][ind]) and (float(combined_df['CPRWidth'][ind]) >= 0.20):
                worked.append('Y')
            else:
                worked.append('N')
        except Exception as e:
            worked.append('-')
    return worked

#Main Logic begins here

#Folder path that contains CSV files of historic market data
folder_path = 'D:\\Docs\\Personal\\Scripts\\historyAnalysis\\Nifty'

#Combile all CSV files
combined_df = combine_csv_files(folder_path)

#Calculate CPR and other pivot points
CPR, CPRh, CPRl, R1, R2, R3, S1, S2, S3, CPRWidth = calculate_cpr_pivots(combined_df)

#Adding new row to append next day's CPR and pivot points
new_row = {' ':'0', 'Date ':'0', 'Open ':'0', 'High ':'0','Low ': '0', 'Close ': '0', 'Shares Traded ': '0', 'Turnover (â‚¹ Cr)' : '0'}
combined_df = combined_df.append(new_row, ignore_index=True)

#Add calculated CPR and pivot points to the dataframe
combined_df['CPR'] = CPR
combined_df['CPRh'] = CPRh
combined_df['CPRl'] = CPRl
combined_df['R1'] = R1
combined_df['R2'] = R2
combined_df['R3'] = R3
combined_df['S1'] = S1
combined_df['S2'] = S2
combined_df['S3'] = S3
combined_df['CPRWidth'] = CPRWidth

#Find the pivot points touched by the proce action and append it to the dataframe
crossed = calPriceVol(combined_df)
combined_df['crossed'] = crossed

# FInd if the price action worked as per the CPR rules and append it to the dataframe
worked = calProbofNarrowCPR(combined_df)
combined_df['worked'] = worked

#Write the final dataframe to CSV file
combined_df.to_csv('combined_file1Nifty.csv')


if not combined_df.empty:
    print("Combined DataFrame:")
    print(combined_df)
else:
    print("No CSV files found in the folder.")
