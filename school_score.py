import pandas as pd
import numpy as np

def get_school_score(enrollment, assessment, effectiveness, csv_path = 'resources/data/df_school_score.csv', csv_save = False):
    
    enrollment = pd.read_csv(enrollment)
    assessment = pd.read_csv(assessment)
    effectiveness = pd.read_csv(effectiveness)
    
     #creation of enrollment scores
    enrollmentdffiltered = enrollment[(enrollment["DistrictOfficialName"] != "All Districts") &  (enrollment["BuildingCode"] != 0) & (enrollment["IHE Type"] == "All") & (enrollment["Subgroup"] == "All Students") & (enrollment["Gender"] == "All Students") & (enrollment["DistrictCode"] != 0) ].rename(columns={'BuildingOfficialName':'BuildingName'})
    enrollmentdffiltered.drop(['SchoolYear', 'ISD Code', 'ISD OfficialName', 'DistrictOfficialName','CharterName', 'CountyCode', 'CountyName', 'EntityType', 'SchoolLevel', 'Locale', 'MISTEM_NAME', 'MISTEM_CODE', 'IHE Type', 'Subgroup', 'Gender', 'Total Graduates (All Students)', 'Total Enrolled in an IHE within 0-6 months', 'Total Enrolled in an IHE within 0-12 months', 'Total % Enrolled in an IHE within 0-12 months', 'Total Enrolled in an IHE within 0-16 months', 'Total % Enrolled in an IHE within 0-16 months', 'Total Enrolled in an IHE within 0-24 months', 'Total % Enrolled in an IHE within 0-24 months', 'Total Enrolled in an IHE within 0-36 months', 'Total % Enrolled in an IHE within 0-36 months', 'Total Enrolled in an IHE within 0-48 months', 'Total % Enrolled in an IHE within 0-48 months', 'Total Accumulating 24 Credits within 0-12 months', 'Total % Accumulating 24 Credits  within 0-12 months', 'Total Accumulating 24 Credits within 0-16 months', 'Total % Accumulating 24 Credits  within 0-16 months', 'Total Accumulating 24 Credits within 0-24 months', 'Total % Accumulating 24 Credits  within 0-24 months', 'Total Accumulating 24 Credits within 0-36 months', 'Total % Accumulating 24 Credits  within 0-36 months', 'Total Accumulating 24 Credits within 0-48 months', 'Total % Accumulating 24 Credits  within 0-48 months'], axis=1, inplace=True)
    enrollmentdffiltered.drop_duplicates(subset=['BuildingCode'], inplace=True)
    
    # creation of assessment scores
    sciassessmentdf = assessment[(assessment["TestType"] == "M-STEP") & (assessment["BuildingCode"] != 00000) & (assessment["ReportCategory"] == "All Students") & (assessment["Subject"] == "Science") ][['DistrictCode','BuildingCode', 'BuildingName','PercentMet']]
    sciassessmentdf.drop_duplicates(subset=['BuildingCode'], inplace=True)
    socassessmentdf = assessment[(assessment["TestType"] == "M-STEP") & (assessment["BuildingCode"] != 00000) & (assessment["ReportCategory"] == "All Students") & (assessment["Subject"] == "Social Studies")  ][['DistrictCode','BuildingCode', 'BuildingName','PercentMet']]
    socassessmentdf.drop_duplicates(subset=['BuildingCode'], inplace=True)
    assessmentdf = sciassessmentdf.merge(socassessmentdf, on=['BuildingCode','BuildingName','DistrictCode'], how='outer', suffixes=('_sci', '_soc'))
    
    #merging dataframes for assessment and enrollment
    totalassessmentdf = assessmentdf.merge(enrollmentdffiltered, on=['BuildingCode','BuildingName'], how='outer')
    totalassessmentdf['DistrictCode'] = totalassessmentdf['DistrictCode_x'].combine_first(totalassessmentdf['DistrictCode_y'])
    totalassessmentdf = totalassessmentdf.drop(['DistrictCode_x','DistrictCode_y'], axis=1)

    #creation of effectiveness score
    effectiveness.rename(columns={"Building Code": "BuildingCode", 'District Code': 'DistrictCode' , 'Building Name': 'BuildingName'}, inplace=True)
    effectivenessdf = effectiveness[(effectiveness["BuildingCode"] != 00000) & (effectiveness["EFFECTIVENESS_CATEGORY"] == "HighlyEffective") ]
    effectivenessdf.drop(['School Year', 'ISD Code', 'ISD Name', 'CountyCode', 'CountyName', 'EntityType', 'SchoolLevel', 'Locale', 'ASSIGNMENT_TYPE', 'STAFF_COUNT', 'TOTAL_COUNT', 'EFFECTIVENESS_CATEGORY', 'District Name'],  axis=1, inplace=True)
    effectivenessdf.rename(columns={"PERCENTAGE": "percentage_highly_effective"}, inplace=True)
    effectivenessdf.drop_duplicates(subset=['BuildingCode'], inplace=True)


    #merging effectiveness score to previous dataframes
    totalassessmentdf = totalassessmentdf.merge(effectivenessdf, on=['BuildingCode','DistrictCode','BuildingName'], how='left')

    #converting from characters that are not numbers and then changing from strings to floats
    totalassessmentdf1 = totalassessmentdf.replace(['>95%','*', '<5%', '< 10', '> 95', '<=5%', '<=20%', '<=50%', '<=10%', '>=80%', '>=50%'], ['95', 'nan','5', '10', '95', '5', '20', '50', '10', '80', '50' ])
    totalassessmentdf1[['DistrictCode','PercentMet_sci',  'PercentMet_soc', 'Total % Enrolled in an IHE within 0-6 months']] =  totalassessmentdf1[['DistrictCode',  'PercentMet_sci',  'PercentMet_soc', 'Total % Enrolled in an IHE within 0-6 months']].astype('float')

    #calculating the amalgam education score for the school building 
    totalassessmentdf1['Total % Enrolled in an IHE within 0-6 months'].mask(totalassessmentdf1['Total % Enrolled in an IHE within 0-6 months'] == 0.05, totalassessmentdf1['Total % Enrolled in an IHE within 0-6 months'].mean(), inplace=True)
    totalassessmentdf1["amalgam_education_score"] = ((1/totalassessmentdf1["Total % Enrolled in an IHE within 0-6 months"]) * (1 / totalassessmentdf1["percentage_highly_effective"])* (1 / totalassessmentdf1["PercentMet_sci"]) * (1 / totalassessmentdf1["PercentMet_soc"]) )
    totalassessmentdf1['amalgam_education_score'] = totalassessmentdf1['amalgam_education_score'].fillna((1/totalassessmentdf1["Total % Enrolled in an IHE within 0-6 months"]) * (1 / totalassessmentdf1["percentage_highly_effective"]) * (1 / totalassessmentdf1["PercentMet_sci"].mean()) * (1 / totalassessmentdf1["PercentMet_soc"].mean()))
    totalassessmentdf1['log_correct_education_score'] = np.log(totalassessmentdf1['amalgam_education_score'])
    
    #removing buildings that do not have a score 
    totalassessmentdf1cleaned = totalassessmentdf1.dropna(subset=['amalgam_education_score']).reset_index(drop = True)
    totalassessmentdf1cleaned = totalassessmentdf1cleaned.rename(columns={'BuildingCode':'building_code','BuildingName':'building_name','PercentMet_soc':'percent_met_soc','Total % Enrolled in an IHE within 0-6 months':'total_enrolled','DistrictCode':'district_code','PercentMet_sci':'percent_met_sci'})
    
    if csv_save:
        totalassessmentdf1cleaned.to_pickle(csv_path)
    return totalassessmentdf1cleaned


if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('enrollment', help='pandas dataFrame')
    parser.add_argument('assessment', help='pandas dataFrame')
    parser.add_argument('effectiveness', help='pandas dataFrame')
    parser.add_argument('csv_save', help='booleon')
    parser.add_argument('csv_path', help='string')
    args = parser.parse_args()

    df_school_score = get_school_score(enrollment = args.enrollment, assessment = args.assessment, effectiveness = args.effectiveness, csv_save = args.csv_save, csv_path = args.csv_path)