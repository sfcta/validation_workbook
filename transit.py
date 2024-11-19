from pathlib import Path

import tomllib, sys
from transit.bart import process_BART_model_outputs
from transit.map_data import process_bart_map, process_muni_map
from transit.muni import process_muni
from transit.obs import process_obs_data
from transit.screen import save_final_screenline_data
from transit.simwrapper_table import process_mkd_bart, process_mkd_muni, process_mkd_screenline
from transit.total_val import process_valTotal_Operator, process_valTotal_Submode
from transit.utils import read_transit_assignments

if __name__ == "__main__":
    toml_path = Path(sys.argv[1])
    print(toml_path)
    
    with open(toml_path, "rb") as f:
        config = tomllib.load(f)

    line_rename = Path(config["input"]["support"]["line_rename"])
    
    model_run_dir = Path(config["input"]['model']["dir"])
    transit_input_dir = Path(config["input"]['observed']["dir"])
    freeflow = model_run_dir / config["input"]["model"]["freeflow"]
    bart_station = Path(config["input"]["observed"]["bart_station"])
    station_node_match = Path(config["input"]["observed"]["station_node_match"])
    bart_county = Path(config["input"]["observed"]["bart_county"])
    bart_screenline = Path(config["input"]["observed"]["bart_screenline"])
    observed_MUNI_Line = Path(config["input"]["observed"]["muni_line"])
    observed_Screenline = Path(config["input"]["observed"]["screenline"])
    observed_NTD = Path(config["input"]["observed"]["ntd"])
    muni_name_match = Path(config["input"]["observed"]["muni_name_match"])
    model_BART = Path(config["bart"]["model_BART"])
    model_BART_county = Path(config["bart"]["model_BART_county"])
    model_BART_Screenline = Path(config["bart"]["model_BART_Screenline"])
    model_MUNI_Line = Path(config["muni"]["model_MUNI_Line"])
    model_Screenline = Path(config["screenline"]["model_Screenline"])
    BART_br_map = Path(config["bart"]["BART_br_map"])
    BART_br_map_pm = Path(config["bart"]["BART_br_map_pm"])
    BART_br_map_am = Path(config["bart"]["BART_br_map_am"])
    BART_at_map = Path(config["bart"]["BART_at_map"])
    BART_at_map_am = Path(config["bart"]["BART_at_map_am"])
    BART_at_map_pm = Path(config["bart"]["BART_at_map_pm"])
    muni_ib_shp = Path(config["muni"]["muni_ib_shp"])
    muni_ob_shp = Path(config["muni"]["muni_ob_shp"])
    obs_MUNI_line_md = Path(config["muni"]["obs_MUNI_line_md"])
    obs_BART_station_md = Path(config["bart"]["obs_BART_station_md"])
    obs_BART_county_md = Path(config["bart"]["obs_BART_county_md"])
    obs_BART_Screenline_md = Path(config["screenline"]["obs_BART_Screenline_md"])
    obs_Screenlines_md = Path(config["screenline"]["obs_Screenlines_md"])
    obs_NTD_md = Path(config["total"]["obs_NTD_md"])
    valTotal_Service = Path(config["total"]["valTotal_Service"])
    valTotal_Operator = Path(config["total"]["valTotal_Operator"])
    valTotal_Submode = Path(config["total"]["valTotal_Submode"])
    BART_br = Path(config["bart"]["BART_br"])
    BART_br_am = Path(config["bart"]["BART_br_am"])
    BART_br_pm = Path(config["bart"]["BART_br_pm"])
    BART_at = Path(config["bart"]["BART_at"])
    BART_at_am = Path(config["bart"]["BART_at_am"])
    BART_at_pm = Path(config["bart"]["BART_at_pm"])
    BART_boarding_allday_csv = Path(config["bart"]["BART_boarding_allday_csv"])
    BART_at_allday_csv = Path(config["bart"]["BART_at_allday_csv"])
    county_br_day_csv = Path(config["bart"]["county_br_day_csv"])
    county_br_am_csv = Path(config["bart"]["county_br_am_csv"])
    county_br_pm_csv = Path(config["bart"]["county_br_pm_csv"])
    county_at_day_csv = Path(config["bart"]["county_at_day_csv"])
    county_at_am_csv = Path(config["bart"]["county_at_am_csv"])
    county_at_pm_csv = Path(config["bart"]["county_at_pm_csv"])
    MUNI_OB = Path(config["muni"]["MUNI_OB"])
    MUNI_IB = Path(config["muni"]["MUNI_IB"])
    MUNI_map_IB = Path(config["muni"]["MUNI_map_IB"])
    MUNI_map_OB = Path(config["muni"]["MUNI_map_OB"])
    MUNI_mode = Path(config["muni"]["MUNI_mode"])
    MUNI_mode_am = Path(config["muni"]["MUNI_mode_am"])
    MUNI_mode_pm = Path(config["muni"]["MUNI_mode_pm"])
    MUNI_tod = Path(config["muni"]["MUNI_tod"])
    MUNI_EB = Path(config["muni"]["MUNI_EB"])
    MUNI_LB = Path(config["muni"]["MUNI_LB"])
    MUNI_Rail = Path(config["muni"]["MUNI_Rail"])
    transbay_BART_IB_csv = Path(config["screenline"]["transbay_BART_IB_csv"])
    transbay_BART_OB_csv = Path(config["screenline"]["transbay_BART_OB_csv"])
    Countyline_BART_IB_csv = Path(config["screenline"]["Countyline_BART_IB_csv"])
    Countyline_BART_OB_csv = Path(config["screenline"]["Countyline_BART_OB_csv"])
    Intra_SF_BART_IB_csv = Path(config["screenline"]["Intra_SF_BART_IB_csv"])
    Intra_SF_BART_OB_csv = Path(config["screenline"]["Intra_SF_BART_OB_csv"])
    transbay_AC_IB_csv = Path(config["screenline"]["transbay_AC_IB_csv"])
    transbay_AC_OB_csv = Path(config["screenline"]["transbay_AC_OB_csv"])
    transbay_overall_IB_csv = Path(config["screenline"]["transbay_overall_IB_csv"])
    transbay_overall_OB_csv = Path(config["screenline"]["transbay_overall_OB_csv"])
    Countyline_CalTrain_IB_csv = Path(config["screenline"]["Countyline_CalTrain_IB_csv"])
    Countyline_CalTrain_OB_csv = Path(config["screenline"]["Countyline_CalTrain_OB_csv"])
    Countyline_SamTrans_IB_csv = Path(config["screenline"]["Countyline_SamTrans_IB_csv"])
    Countyline_SamTrans_OB_csv = Path(config["screenline"]["Countyline_SamTrans_OB_csv"])
    Countyline_overall_IB_csv = Path(config["screenline"]["Countyline_overall_IB_csv"])
    Countyline_overall_OB_csv = Path(config["screenline"]["Countyline_overall_OB_csv"])
    GG_Transit_IB_csv = Path(config["screenline"]["GG_Transit_IB_csv"])
    GG_Transit_OB_csv = Path(config["screenline"]["GG_Transit_OB_csv"])
    GG_Ferry_IB_csv = Path(config["screenline"]["GG_Ferry_IB_csv"])
    GG_Ferry_OB_csv = Path(config["screenline"]["GG_Ferry_OB_csv"])
    GG_overall_IB_csv = Path(config["screenline"]["GG_overall_IB_csv"])
    GG_overall_OB_csv = Path(config["screenline"]["GG_overall_OB_csv"])
    MUNI_ib_day = Path(config["muni"]["MUNI_ib_day"])
    MUNI_ob_day = Path(config["muni"]["MUNI_ob_day"])
    MUNI_ib_am = Path(config["muni"]["MUNI_ib_am"])
    MUNI_ib_pm = Path(config["muni"]["MUNI_ib_pm"])
    MUNI_ob_am = Path(config["muni"]["MUNI_ob_am"])
    MUNI_ob_pm = Path(config["muni"]["MUNI_ob_pm"])
    MUNI_mode_day = Path(config["muni"]["MUNI_mode_day"])
    MUNI_mode_am_md = Path(config["muni"]["MUNI_mode_am_md"])
    MUNI_mode_pm_md = Path(config["muni"]["MUNI_mode_pm_md"])
    MUNI_tod_md = Path(config["muni"]["MUNI_tod_md"])
    MUNI_EB_md = Path(config["muni"]["MUNI_EB_md"])
    MUNI_LB_md = Path(config["muni"]["MUNI_LB_md"])
    MUNI_Rail_md = Path(config["muni"]["MUNI_Rail_md"])
    BART_boarding_allday_md = Path(config["bart"]["BART_boarding_allday_md"])
    BART_boarding_am_md = Path(config["bart"]["BART_boarding_am_md"])
    BART_boarding_pm_md = Path(config["bart"]["BART_boarding_pm_md"])
    BART_at_allday_md = Path(config["bart"]["BART_at_allday_md"])
    BART_at_am_md = Path(config["bart"]["BART_at_am_md"])
    BART_at_pm_md = Path(config["bart"]["BART_at_pm_md"])
    county_br_day_md = Path(config["bart"]["county_br_day_md"])
    county_br_am_md = Path(config["bart"]["county_br_am_md"])
    county_br_pm_md = Path(config["bart"]["county_br_pm_md"])
    county_at_day_md = Path(config["bart"]["county_at_day_md"])
    county_at_am_md = Path(config["bart"]["county_at_am_md"])
    county_at_pm_md = Path(config["bart"]["county_at_pm_md"])
    transbay_BART_IB_md = Path(config["screenline"]["transbay_BART_IB_md"])
    transbay_BART_OB_md = Path(config["screenline"]["transbay_BART_OB_md"])
    Countyline_BART_OB_md = Path(config["screenline"]["Countyline_BART_OB_md"])
    Countyline_BART_IB_md = Path(config["screenline"]["Countyline_BART_IB_md"])
    SF_out_md = Path(config["screenline"]["SF_out_md"])
    SF_in_md = Path(config["screenline"]["SF_in_md"])
    transbay_AC_IB_md = Path(config["screenline"]["transbay_AC_IB_md"])
    transbay_AC_OB_md = Path(config["screenline"]["transbay_AC_OB_md"])
    transbay_overall_IB_md = Path(config["screenline"]["transbay_overall_IB_md"])
    transbay_overall_OB_md = Path(config["screenline"]["transbay_overall_OB_md"])
    Countyline_CalTrain_IB_md = Path(config["screenline"]["Countyline_CalTrain_IB_md"])
    Countyline_CalTrain_OB_md = Path(config["screenline"]["Countyline_CalTrain_OB_md"])
    Countyline_SamTrans_IB_md = Path(config["screenline"]["Countyline_SamTrans_IB_md"])
    Countyline_SamTrans_OB_md = Path(config["screenline"]["Countyline_SamTrans_OB_md"])
    Countyline_overall_IB_md = Path(config["screenline"]["Countyline_overall_IB_md"])
    Countyline_overall_OB_md = Path(config["screenline"]["Countyline_overall_OB_md"])
    GG_Transit_IB_md = Path(config["screenline"]["GG_Transit_IB_md"])
    GG_Transit_OB_md = Path(config["screenline"]["GG_Transit_OB_md"])
    GG_Ferry_IB_md = Path(config["screenline"]["GG_Ferry_IB_md"])
    GG_Ferry_OB_md = Path(config["screenline"]["GG_Ferry_OB_md"])
    GG_overall_IB_md = Path(config["screenline"]["GG_overall_IB_md"])
    GG_overall_OB_md = Path(config["screenline"]["GG_overall_OB_md"])
    GG_Transit = config["screenline"]["GG_Transit"]
    SamTrans = config["screenline"]["SamTrans"]
    GG_Ferry = config["screenline"]["GG_Ferry"]
    CalTrain = config["screenline"]["CalTrain"]
    AC_transit = config["screenline"]["AC_transit"]
    valTotal_Submode_md = Path(config["total"]["valTotal_Submode_md"])
    valTotal_Service_md = Path(config["total"]["valTotal_Service_md"])
    valTotal_Operator_md = Path(config["total"]["valTotal_Operator_md"])
    transbay_node = config["screenline"]["transbay_node"]
    countyline_node = config["screenline"]["countyline_node"]
    output_dir = Path(config["output"]["dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    time_periods = ["EA", "AM", "MD", "PM", "EV"]
    tod_order = ["EA", "AM", "MD", "PM", "EV", "Total"]
    
    combined_gdf = read_transit_assignments(model_run_dir, time_periods)
    process_BART_model_outputs(
        combined_gdf,
        output_dir,
        transit_input_dir,
        station_node_match,
        model_BART_Screenline,
        model_BART_county,
        model_BART,
        transbay_node,
        countyline_node,
    )
    process_muni(
        combined_gdf,
        muni_name_match,
        line_rename,
        transit_input_dir,
        observed_MUNI_Line,
        output_dir,
        model_MUNI_Line,
    )
    save_final_screenline_data(
        combined_gdf,
        output_dir,
        model_BART_Screenline,
        model_Screenline,
        SamTrans,
        GG_Transit,
        GG_Ferry,
        CalTrain,
        AC_transit,
    )
    process_mkd_muni(
        transit_input_dir,
        observed_MUNI_Line,
        output_dir,
        model_MUNI_Line,
        output_dir,
        output_dir,
        MUNI_ib_day,
        MUNI_ob_day,
        MUNI_ib_am,
        MUNI_ib_pm,
        MUNI_ob_am,
        MUNI_ob_pm,
        MUNI_mode_day,
        MUNI_mode,
        MUNI_mode_am_md,
        MUNI_mode_am,
        MUNI_mode_pm_md,
        MUNI_mode_pm,
        MUNI_tod_md,
        MUNI_tod,
        MUNI_EB_md,
        MUNI_EB,
        MUNI_LB_md,
        MUNI_LB,
        MUNI_Rail_md,
        MUNI_Rail,
        MUNI_IB,
        MUNI_OB,
    )
    process_mkd_bart(
        transit_input_dir,
        bart_station,
        output_dir,
        model_BART,
        output_dir,
        output_dir,
        bart_county,
        model_BART_county,
        bart_screenline,
        output_dir,
        tod_order,
        BART_boarding_allday_md,
        BART_boarding_am_md,
        BART_boarding_pm_md,
        BART_at_allday_md,
        BART_at_am_md,
        BART_at_pm_md,
        BART_boarding_allday_csv,
        BART_at_allday_csv,
        county_br_day_csv,
        county_br_am_csv,
        county_br_pm_csv,
        county_at_day_csv,
        county_at_am_csv,
        county_at_pm_csv,
        model_BART_Screenline,
        county_br_day_md,
        county_br_am_md,
        county_br_pm_md,
        county_at_day_md,
        county_at_am_md,
        county_at_pm_md,
        transbay_BART_IB_md,
        transbay_BART_OB_md,
        Countyline_BART_OB_md,
        Countyline_BART_IB_md,
        SF_out_md,
        SF_in_md,
        transbay_BART_IB_csv,
        transbay_BART_OB_csv,
        Countyline_BART_IB_csv,
        Countyline_BART_OB_csv,
        Intra_SF_BART_IB_csv,
        Intra_SF_BART_OB_csv,
    )
    process_mkd_screenline(
        transit_input_dir,
        observed_Screenline,
        output_dir,
        model_Screenline,
        output_dir,
        tod_order,
        output_dir,
        transbay_AC_IB_md,
        transbay_AC_OB_md,
        transbay_overall_IB_md,
        transbay_overall_OB_md,
        Countyline_CalTrain_IB_md,
        Countyline_CalTrain_OB_md,
        Countyline_SamTrans_IB_md,
        Countyline_SamTrans_OB_md,
        Countyline_overall_IB_md,
        Countyline_overall_OB_md,
        GG_Transit_IB_md,
        GG_Transit_OB_md,
        GG_Ferry_IB_md,
        GG_Ferry_OB_md,
        GG_overall_IB_md,
        GG_overall_OB_md,
        transbay_AC_IB_csv,
        transbay_AC_OB_csv,
        transbay_overall_IB_csv,
        transbay_overall_OB_csv,
        Countyline_CalTrain_IB_csv,
        Countyline_CalTrain_OB_csv,
        Countyline_SamTrans_IB_csv,
        Countyline_SamTrans_OB_csv,
        Countyline_overall_IB_csv,
        Countyline_overall_OB_csv,
        GG_Transit_IB_csv,
        GG_Transit_OB_csv,
        GG_Ferry_IB_csv,
        GG_Ferry_OB_csv,
        GG_overall_IB_csv,
        GG_overall_OB_csv,
    )
    process_muni_map(
        combined_gdf,
        output_dir,
        output_dir,
        output_dir,
        freeflow,
        model_MUNI_Line,
        muni_ib_shp,
        muni_ob_shp,
        MUNI_OB,
        MUNI_IB,
        MUNI_map_IB,
        MUNI_map_OB,
    )
    process_bart_map(
        output_dir,
        transit_input_dir,
        output_dir,
        bart_station,
        model_BART,
        output_dir,
        BART_br,
        BART_br_map,
        BART_br_pm,
        BART_br_map_pm,
        BART_br_am,
        BART_br_map_am,
        BART_at,
        BART_at_map,
        BART_at_am,
        BART_at_map_am,
        BART_at_pm,
        BART_at_map_pm,
        station_node_match,
    )
    process_obs_data(
        transit_input_dir,
        output_dir,
        observed_MUNI_Line,
        bart_station,
        bart_county,
        bart_screenline,
        observed_Screenline,
        observed_NTD,
        obs_MUNI_line_md,
        obs_BART_station_md,
        obs_BART_county_md,
        obs_BART_Screenline_md,
        obs_Screenlines_md,
        obs_NTD_md,
    )
    process_valTotal_Operator(
        combined_gdf,
        transit_input_dir,
        output_dir,
        output_dir,
        observed_NTD,
        valTotal_Operator_md,
        valTotal_Operator,
    )
    process_valTotal_Submode(
        combined_gdf,
        transit_input_dir,
        output_dir,
        output_dir,
        observed_NTD,
        valTotal_Submode,
        valTotal_Submode_md,
        valTotal_Service_md,
        valTotal_Service,
    )
