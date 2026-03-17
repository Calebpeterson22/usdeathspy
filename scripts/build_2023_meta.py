import pandas as pd
import os

def generate_2023_metadata():
    meta_data = [
        {"name": "reserved_1", "start": 1, "end": 18, "size": 18, "type": "str", "description": "Reserved positions", "codes": ""},
        {"name": "record_type", "start": 19, "end": 19, "size": 1, "type": "int", "description": "Record type", "codes": "1=Residents|2=Nonresidents"},
        {"name": "resident_status", "start": 20, "end": 20, "size": 1, "type": "int", "description": "Resident status (US Occurrence)", "codes": "1=Residents|2=Intrastate Nonresidents|3=Interstate Nonresidents|4=Foreign Residents"},
        {"name": "reserved_2", "start": 21, "end": 62, "size": 42, "type": "str", "description": "Reserved positions", "codes": ""},
        {"name": "education", "start": 63, "end": 63, "size": 1, "type": "int", "description": "Education (2003 revision)", "codes": "1=8th grade or less|2=9-12th grade no diploma|3=High school graduate or GED|4=Some college no degree|5=Associate degree|6=Bachelor's degree|7=Master's degree|8=Doctorate or professional degree|9=Unknown"},
        {"name": "education_report_flag", "start": 64, "end": 64, "size": 1, "type": "int", "description": "Education reporting flag", "codes": "1=2003 revision|2=No education item"},
        {"name": "month_of_death", "start": 65, "end": 66, "size": 2, "type": "int", "description": "Month of death", "codes": "01=January|02=February|03=March|04=April|05=May|06=June|07=July|08=August|09=September|10=October|11=November|12=December"},
        {"name": "reserved_3", "start": 67, "end": 68, "size": 2, "type": "str", "description": "Reserved positions", "codes": ""},
        {"name": "sex", "start": 69, "end": 69, "size": 1, "type": "str", "description": "Sex", "codes": "M=Male|F=Female"},
        {"name": "detail_age", "start": 70, "end": 73, "size": 4, "type": "int", "description": "Detail age", "codes": "1=Years|2=Months|4=Days|5=Hours|6=Minutes|9=Age not stated"},
        {"name": "age_substitution_flag", "start": 74, "end": 74, "size": 1, "type": "str", "description": "Age substitution flag", "codes": "1=Calculated age substituted|blank=Not substituted"},
        {"name": "age_recode_52", "start": 75, "end": 76, "size": 2, "type": "int", "description": "Age recode 52", "codes": "01=Under 1 hour|52=Age not stated"},
        {"name": "age_recode_27", "start": 77, "end": 78, "size": 2, "type": "int", "description": "Age recode 27", "codes": "01=Under 1 month|27=Age not stated"},
        {"name": "age_recode_12", "start": 79, "end": 80, "size": 2, "type": "int", "description": "Age recode 12", "codes": "01=Under 1 year|12=Age not stated"},
        {"name": "infant_age_recode_22", "start": 81, "end": 82, "size": 2, "type": "int", "description": "Infant age recode 22", "codes": "01=Under 1 hour|22=11 months"},
        {"name": "place_of_death", "start": 83, "end": 83, "size": 1, "type": "int", "description": "Place of death", "codes": "1=Hospital inpatient|2=ER|3=DOA|4=Home|5=Hospice|6=Nursing home|7=Other|9=Unknown"},
        {"name": "marital_status", "start": 84, "end": 84, "size": 1, "type": "str", "description": "Marital status", "codes": "S=Single|M=Married|W=Widowed|D=Divorced|U=Unknown"},
        {"name": "day_of_week_of_death", "start": 85, "end": 85, "size": 1, "type": "int", "description": "Day of week of death", "codes": "1=Sun|2=Mon|3=Tue|4=Wed|5=Thu|6=Fri|7=Sat|9=Unknown"},
        {"name": "reserved_4", "start": 86, "end": 101, "size": 16, "type": "str", "description": "Reserved positions", "codes": ""},
        {"name": "current_data_year", "start": 102, "end": 105, "size": 4, "type": "int", "description": "Current data year", "codes": "2023=2023"},
        {"name": "injury_at_work", "start": 106, "end": 106, "size": 1, "type": "str", "description": "Injury at work", "codes": "Y=Yes|N=No|U=Unknown"},
        {"name": "manner_of_death", "start": 107, "end": 107, "size": 1, "type": "str", "description": "Manner of death", "codes": "1=Accident|2=Suicide|3=Homicide|4=Pending|5=Could not determine|6=Self-inflicted|7=Natural"},
        {"name": "method_of_disposition", "start": 108, "end": 108, "size": 1, "type": "str", "description": "Method of disposition", "codes": "B=Burial|C=Cremation|D=Donation|E=Entombment|O=Other|R=Removal|U=Unknown"},
        {"name": "autopsy", "start": 109, "end": 109, "size": 1, "type": "str", "description": "Autopsy", "codes": "Y=Yes|N=No|U=Unknown"},
        {"name": "reserved_5", "start": 110, "end": 143, "size": 34, "type": "str", "description": "Reserved positions", "codes": ""},
        {"name": "activity_code", "start": 144, "end": 144, "size": 1, "type": "str", "description": "Activity code", "codes": "0=Sports|1=Leisure|2=Work|3=Other work|4=Resting|8=Other|9=Unspecified"},
        {"name": "place_of_injury", "start": 145, "end": 145, "size": 1, "type": "str", "description": "Place of injury", "codes": "0=Home|1=Institution|2=Public|3=Sports|4=Street|5=Trade|6=Industrial|7=Farm|8=Other|9=Unspecified"},
        {"name": "icd_code", "start": 146, "end": 149, "size": 4, "type": "str", "description": "ICD-10 code", "codes": ""},
        {"name": "cause_recode_358", "start": 150, "end": 152, "size": 3, "type": "int", "description": "358 cause recode", "codes": ""},
        {"name": "reserved_6", "start": 153, "end": 153, "size": 1, "type": "str", "description": "Reserved position", "codes": ""},
        {"name": "cause_recode_113", "start": 154, "end": 156, "size": 3, "type": "int", "description": "113 cause recode", "codes": ""},
        {"name": "infant_cause_recode_130", "start": 157, "end": 159, "size": 3, "type": "int", "description": "130 infant cause recode", "codes": ""},
        {"name": "cause_recode_39", "start": 160, "end": 161, "size": 2, "type": "int", "description": "39 cause recode", "codes": ""},
        {"name": "reserved_7", "start": 162, "end": 162, "size": 1, "type": "str", "description": "Reserved position", "codes": ""},
        {"name": "num_entity_axis_conditions", "start": 163, "end": 164, "size": 2, "type": "int", "description": "Number of entity-axis conditions", "codes": ""},
        {"name": "entity_axis_conditions", "start": 165, "end": 304, "size": 140, "type": "str", "description": "Entity-axis conditions", "codes": ""},
        {"name": "reserved_8", "start": 305, "end": 340, "size": 36, "type": "str", "description": "Reserved positions", "codes": ""},
        {"name": "num_record_axis_conditions", "start": 341, "end": 342, "size": 2, "type": "int", "description": "Number of record-axis conditions", "codes": ""},
        {"name": "reserved_9", "start": 343, "end": 343, "size": 1, "type": "str", "description": "Reserved position", "codes": ""},
        {"name": "record_axis_conditions", "start": 344, "end": 443, "size": 100, "type": "str", "description": "Record-axis conditions", "codes": ""},
        {"name": "reserved_10", "start": 444, "end": 447, "size": 4, "type": "str", "description": "Reserved positions", "codes": ""},
        {"name": "race_imputation_flag", "start": 448, "end": 448, "size": 1, "type": "str", "description": "Race imputation flag", "codes": "1=Unknown imputed|2=Other imputed"},
        {"name": "reserved_11", "start": 449, "end": 449, "size": 1, "type": "str", "description": "Reserved position", "codes": ""},
        {"name": "race_recode_6", "start": 450, "end": 450, "size": 1, "type": "int", "description": "Race recode 6", "codes": "1=White|2=Black|3=AIAN|4=Asian|5=NHOPI|6=More than one"},
        {"name": "reserved_12", "start": 451, "end": 483, "size": 33, "type": "str", "description": "Reserved positions", "codes": ""},
        {"name": "hispanic_origin", "start": 484, "end": 486, "size": 3, "type": "int", "description": "Hispanic origin", "codes": "100-199=Non-Hispanic|200-299=Hispanic"},
        {"name": "hispanic_origin_race_recode", "start": 487, "end": 488, "size": 2, "type": "int", "description": "Hispanic origin/race recode", "codes": "01=Mexican|02=Puerto Rico|08=NH White|09=NH Black"},
        {"name": "race_recode_40", "start": 489, "end": 490, "size": 2, "type": "int", "description": "Race recode 40", "codes": "01=White|02=Black|03=AIAN"},
        {"name": "reserved_13", "start": 491, "end": 805, "size": 315, "type": "str", "description": "Reserved positions", "codes": ""},
        {"name": "occupation_4digit", "start": 806, "end": 809, "size": 4, "type": "str", "description": "Occupation 4-digit Census code", "codes": ""},
        {"name": "occupation_recode", "start": 810, "end": 811, "size": 2, "type": "int", "description": "Occupation recode", "codes": "01=Management|26=Other housewife"},
        {"name": "industry_4digit", "start": 812, "end": 815, "size": 4, "type": "str", "description": "Industry 4-digit Census code", "codes": ""},
        {"name": "industry_recode", "start": 816, "end": 817, "size": 2, "type": "int", "description": "Industry recode", "codes": "01=Agri|23=Military/Other"}
    ]

    df = pd.DataFrame(meta_data)


    base_path = os.path.dirname(os.path.abspath(__file__)) # Gets the /scripts/ folder
    target_dir = os.path.join(base_path, "..", "src", "usdeathspy", "metadata")
    os.makedirs(target_dir, exist_ok=True)
    
    file_path = os.path.join(target_dir, "mortality_multiple_2023.csv")
    df.to_csv(file_path, index=False)
    print(f"Generated {file_path}")

if __name__ == "__main__":
    generate_2023_metadata()