import pandas as pd
import numpy as np

def get_district_score(atrisk, enrollment, assessment, csv_path = 'resources/data/df_district_score.csv', csv_save = False):

    atrisk = pd.read_csv(atrisk) 
    enrollment = pd.read_csv(enrollment)
    assessment = pd.read_csv(assessment)
    
    enrollmentdffiltered = enrollment[(enrollment["BuildingOfficialName"] == "All Buildings") & (enrollment["IHE Type"] == "All") & (enrollment["Subgroup"] == "All Students") & (enrollment["Gender"] == "All Students") & (enrollment["DistrictCode"] != 0) ]
    enrollmentdffiltered.drop(['SchoolYear', 'ISD Code', 'ISD OfficialName', 'DistrictOfficialName', 'BuildingCode', 'BuildingOfficialName', 'CharterName', 'CountyCode', 'CountyName', 'EntityType', 'SchoolLevel', 'Locale', 'MISTEM_NAME', 'MISTEM_CODE', 'IHE Type', 'Subgroup', 'Gender', 'Total Graduates (All Students)', 'Total Enrolled in an IHE within 0-6 months', 'Total Enrolled in an IHE within 0-12 months', 'Total % Enrolled in an IHE within 0-12 months', 'Total Enrolled in an IHE within 0-16 months', 'Total % Enrolled in an IHE within 0-16 months', 'Total Enrolled in an IHE within 0-24 months', 'Total % Enrolled in an IHE within 0-24 months', 'Total Enrolled in an IHE within 0-36 months', 'Total % Enrolled in an IHE within 0-36 months', 'Total Enrolled in an IHE within 0-48 months', 'Total % Enrolled in an IHE within 0-48 months', 'Total Accumulating 24 Credits within 0-12 months', 'Total % Accumulating 24 Credits  within 0-12 months', 'Total Accumulating 24 Credits within 0-16 months', 'Total % Accumulating 24 Credits  within 0-16 months', 'Total Accumulating 24 Credits within 0-24 months', 'Total % Accumulating 24 Credits  within 0-24 months', 'Total Accumulating 24 Credits within 0-36 months', 'Total % Accumulating 24 Credits  within 0-36 months', 'Total Accumulating 24 Credits within 0-48 months', 'Total % Accumulating 24 Credits  within 0-48 months'], axis=1, inplace=True)
    
    #creation of assessment score
    sciassessmentdf = assessment[(assessment["TestType"] == "M-STEP") & (assessment["BuildingName"] == "All Buildings") & (assessment["ReportCategory"] == "All Students") & (assessment["Subject"] == "Science") & (assessment["DistrictCode"] != 00000) ][['DistrictCode','PercentMet']]
    socassessmentdf = assessment[(assessment["TestType"] == "M-STEP") & (assessment["BuildingName"] == "All Buildings") & (assessment["ReportCategory"] == "All Students") & (assessment["Subject"] == "Social Studies") & (assessment["DistrictCode"] != 00000) ][['DistrictCode','PercentMet']]
    assessmentdf = sciassessmentdf.merge(socassessmentdf, on='DistrictCode', how='outer', suffixes=('_sci', '_soc'))
    
    #merging of scores  
    assessmentandenrollmentdf = assessmentdf.merge(enrollmentdffiltered, on='DistrictCode', how='outer')
    atriskdf = atrisk[['DistrictCode','CollegeNotReady']]
    totalassessmentdf = atriskdf.merge(assessmentandenrollmentdf, on='DistrictCode', how='left') 
    
    #converting from characters that are not numbers and then changing from strings to floats
    totalassessmentdf1 = totalassessmentdf.replace(['>95%','*', '<5%', '< 10', '> 95', '<=5%', '<=20%', '<=50%', '<=10%', '>=80%', '>=50%'], ['95', 'nan','5', '10', '95', '5', '20', '50', '10', '80', '50' ])
    totalassessmentdf1[['DistrictCode', 'CollegeNotReady', 'PercentMet_sci',  'PercentMet_soc', 'Total % Enrolled in an IHE within 0-6 months']] =  totalassessmentdf1[['DistrictCode', 'CollegeNotReady', 'PercentMet_sci',  'PercentMet_soc', 'Total % Enrolled in an IHE within 0-6 months']].astype('float')
    
    #calculating the amalgam education score for the district
    totalassessmentdf1['Total % Enrolled in an IHE within 0-6 months'].mask(totalassessmentdf1['Total % Enrolled in an IHE within 0-6 months'] == 0.05, totalassessmentdf1['Total % Enrolled in an IHE within 0-6 months'].mean(), inplace=True)
    totalassessmentdf1["amalgam_education_score"] = ((1/totalassessmentdf1["Total % Enrolled in an IHE within 0-6 months"]) * (1 / totalassessmentdf1["PercentMet_sci"]) * (1 / totalassessmentdf1["PercentMet_soc"]))
    totalassessmentdf1['amalgam_education_score'] = totalassessmentdf1['amalgam_education_score'].fillna((1/totalassessmentdf1["Total % Enrolled in an IHE within 0-6 months"]) * (1 / totalassessmentdf1["PercentMet_sci"].mean()) * (1 / totalassessmentdf1["PercentMet_soc"].mean()))
    totalassessmentdf1['log_correct_education_score'] = np.log(totalassessmentdf1['amalgam_education_score'])
    #totalassessmentdf1.drop(['SchoolYear', 'IsdCode', 'IsdName', 'BuildingCode', 'BuildingName', 'CountyCode', 'CountyName', 'EntityType', 'Local', 'MISTEM_NAME', 'MISTEM_CODE', 'ELAGr3Prof31aCnt', 'ELAGr3Prof31a', 'ELAGr3NotProf31aCnt', 'ELAGr3NotProf31a', 'MathGr8Prof31aCnt', 'MathGr8Prof31a', 'MathGr8NotProf31aCnt', 'MathGr8NotProf31a', 'CollegeReady31aCnt', 'CollegeReady31a', 'CollegeNotReadyCnt', 'SchoolYear_x', 'TestType_x', 'TestPopulation_x', 'ISDCode_x', 'ISDName_x', 'DistrictName_x', 'BuildingCode_x', 'BuildingName_x', 'CountyCode_x', 'CountyName_x', 'EntityType_x', 'SchoolLevel_x', 'Locale_x', 'MISTEM_NAME_x', 'MISTEM_CODE_x', 'GradeContentTested_x', 'ReportCategory_x', 'TotalAdvanced_x', 'TotalProficient_x', 'TotalPartiallyProficient_x', 'TotalNotProficient_x', 'TotalSurpassed_x', 'TotalAttained_x', 'TotalEmergingTowards_x', 'TotalMet_x', 'TotalDidNotMeet_x', 'NumberAssessed_x', 'PercentAdvanced_x', 'PercentProficient_x', 'PercentPartiallyProficient_x', 'PercentNotProficient_x', 'PercentSurpassed_x', 'PercentAttained_x', 'PercentEmergingTowards_x', 'PercentDidNotMeet_x', 'AvgSS_x', 'StdDevSS_x', 'MeanPtsEarned_x', 'MinScaleScore_x', 'MaxScaleScore_x', 'ScaleScore25_x', 'ScaleScore50_x', 'ScaleScore75_x', 'SchoolYear_y', 'TestType_y', 'TestPopulation_y', 'ISDCode_y', 'ISDName_y', 'DistrictName_y', 'BuildingCode_y', 'BuildingName_y', 'CountyCode_y', 'CountyName_y', 'EntityType_y', 'SchoolLevel_y', 'Locale_y', 'MISTEM_NAME_y', 'MISTEM_CODE_y', 'GradeContentTested_y', 'ReportCategory_y', 'TotalAdvanced_y', 'TotalProficient_y', 'TotalPartiallyProficient_y', 'TotalNotProficient_y', 'TotalSurpassed_y', 'TotalAttained_y', 'TotalEmergingTowards_y', 'TotalMet_y', 'TotalDidNotMeet_y', 'NumberAssessed_y', 'PercentAdvanced_y', 'PercentProficient_y', 'PercentPartiallyProficient_y', 'PercentNotProficient_y', 'PercentSurpassed_y', 'PercentAttained_y', 'PercentEmergingTowards_y', 'PercentDidNotMeet_y', 'AvgSS_y', 'StdDevSS_y', 'MeanPtsEarned_y', 'MinScaleScore_y', 'MaxScaleScore_y', 'ScaleScore25_y', 'ScaleScore50_y', 'ScaleScore75_y'], axis=1, inplace=True)
    totalassessmentdf1 = totalassessmentdf1.rename(columns={'PercentMet_soc':'percent_met_soc','Total % Enrolled in an IHE within 0-6 months':'total_enrolled','DistrictCode':'district_code','PercentMet_sci':'percent_met_sci', 'CollegeNotReady':'college_not_ready', 'amalgam_education_score':'amalgam_education_score','log_correct_education_score':'log_correct_education_score'})
    totalassessmentdf1 = totalassessmentdf1.dropna(subset=['amalgam_education_score']).reset_index(drop = True)

    if csv_save:
        totalassessmentdf1.to_csv(csv_path, index=False, encoding="utf-8")
    return totalassessmentdf1


if __name__=='__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('enrollment', help='pandas dataFrame')
    parser.add_argument('assessment', help='pandas dataFrame')
    parser.add_argument('atrisk', help='pandas dataFrame')
    parser.add_argument('csv_save', help='booleon')
    parser.add_argument('csv_path', help='string')
    args = parser.parse_args()

    df_district_score = get_district_score(enrollment = args.enrollment, assessment = args.assessment, atrisk = args.atrisk, csv_save = args.csv_save, csv_path = args.csv_path)
