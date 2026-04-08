import streamlit as st
from snowflake.snowpark.context import get_active_session
import altair as alt

session = get_active_session()

st.title("Healthcare RxDecision Data Hub")
st.caption("Powered by Snowflake Marketplace - Definitive Healthcare RxDecision Insights")

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "Overview", "Patient Explorer", "ICU Vitals", "Billing",
    "Rx Decisions", "Hospitals & Providers", "AI Risk Scoring"
])

with tab1:
    st.header("Platform Overview")
    c1, c2, c3, c4 = st.columns(4)
    patients = session.sql("SELECT COUNT(*) AS CNT FROM RAW_DB.RAW_SCHEMA.PATIENT_RAW").collect()[0]["CNT"]
    icu = session.sql("SELECT COUNT(*) AS CNT FROM RAW_DB.RAW_SCHEMA.ICU_EVENTS").collect()[0]["CNT"]
    bills = session.sql("SELECT COUNT(*) AS CNT FROM RAW_DB.RAW_SCHEMA.BILLING_DATA").collect()[0]["CNT"]
    rx = session.sql("SELECT COUNT(*) AS CNT FROM RAW_DB.RAW_SCHEMA.PRESCRIPTION_DATA").collect()[0]["CNT"]
    c1.metric("Total Patients", f"{patients:,}")
    c2.metric("ICU Events", f"{icu:,}")
    c3.metric("Billing Records", f"{bills:,}")
    c4.metric("Rx Decisions (Marketplace)", f"{rx:,}")

    st.subheader("Admissions by Diagnosis")
    diag_df = session.sql("SELECT DIAGNOSIS, COUNT(*) AS PATIENT_COUNT FROM RAW_DB.RAW_SCHEMA.PATIENT_RAW GROUP BY DIAGNOSIS ORDER BY PATIENT_COUNT DESC").to_pandas()
    chart = alt.Chart(diag_df).mark_bar(color="#4A90D9").encode(x=alt.X("PATIENT_COUNT:Q", title="Patients"), y=alt.Y("DIAGNOSIS:N", sort="-x", title="Diagnosis")).properties(height=300)
    st.altair_chart(chart, use_container_width=True)

    st.subheader("Admission Trend (Last 12 Months)")
    trend_df = session.sql("SELECT DATE_TRUNC('MONTH', ADMISSION_DATE) AS MONTH, COUNT(*) AS ADMISSIONS FROM RAW_DB.RAW_SCHEMA.PATIENT_RAW GROUP BY MONTH ORDER BY MONTH").to_pandas()
    trend_chart = alt.Chart(trend_df).mark_area(opacity=0.6, color="#4A90D9").encode(x=alt.X("MONTH:T", title="Month"), y=alt.Y("ADMISSIONS:Q", title="Admissions")).properties(height=250)
    st.altair_chart(trend_chart, use_container_width=True)

with tab2:
    st.header("Patient Explorer")
    fc1, fc2 = st.columns(2)
    with fc1:
        diag_filter = st.selectbox("Diagnosis", ["All"] + session.sql("SELECT DISTINCT DIAGNOSIS FROM RAW_DB.RAW_SCHEMA.PATIENT_RAW ORDER BY DIAGNOSIS").to_pandas()["DIAGNOSIS"].tolist())
    with fc2:
        gender_filter = st.selectbox("Gender", ["All", "M", "F"])
    where = "WHERE 1=1"
    if diag_filter != "All":
        where += f" AND DIAGNOSIS = '{diag_filter}'"
    if gender_filter != "All":
        where += f" AND GENDER = '{gender_filter}'"
    patient_df = session.sql(f"SELECT PATIENT_ID, NAME, AGE, GENDER, DIAGNOSIS, ADMISSION_DATE FROM RAW_DB.RAW_SCHEMA.PATIENT_RAW {where} ORDER BY ADMISSION_DATE DESC LIMIT 500").to_pandas()
    st.dataframe(patient_df, use_container_width=True, height=400)

    st.subheader("Age Distribution")
    age_df = session.sql(f"SELECT CASE WHEN AGE < 30 THEN '18-29' WHEN AGE < 50 THEN '30-49' WHEN AGE < 70 THEN '50-69' ELSE '70+' END AS AGE_GROUP, COUNT(*) AS CNT FROM RAW_DB.RAW_SCHEMA.PATIENT_RAW {where} GROUP BY AGE_GROUP ORDER BY AGE_GROUP").to_pandas()
    st.altair_chart(alt.Chart(age_df).mark_bar(color="#6C5CE7").encode(x="AGE_GROUP:N", y="CNT:Q").properties(height=250), use_container_width=True)

with tab3:
    st.header("ICU Vitals Monitor")
    m1, m2, m3, m4 = st.columns(4)
    icu_stats = session.sql("SELECT ROUND(AVG(HEART_RATE),1) AS AVG_HR, ROUND(AVG(OXYGEN_LEVEL),1) AS AVG_O2, SUM(CASE WHEN IS_CRITICAL THEN 1 ELSE 0 END) AS CRITICAL, COUNT(*) AS TOTAL FROM TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_ICU_EVENTS").collect()[0]
    m1.metric("Avg Heart Rate", f"{icu_stats['AVG_HR']} bpm")
    m2.metric("Avg O2 Level", f"{icu_stats['AVG_O2']}%")
    m3.metric("Critical Events", f"{icu_stats['CRITICAL']:,}")
    m4.metric("Total Events", f"{icu_stats['TOTAL']:,}")

    st.subheader("Event Type Breakdown")
    evt_df = session.sql("SELECT EVENT_TYPE, COUNT(*) AS CNT, SUM(CASE WHEN IS_CRITICAL THEN 1 ELSE 0 END) AS CRITICAL_CNT FROM TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_ICU_EVENTS GROUP BY EVENT_TYPE ORDER BY CNT DESC").to_pandas()
    st.altair_chart(alt.Chart(evt_df).mark_bar().encode(x=alt.X("CNT:Q", title="Count"), y=alt.Y("EVENT_TYPE:N", sort="-x"), color=alt.value("#00B894")).properties(height=250), use_container_width=True)

    st.subheader("ICU Events Over Time")
    icu_trend = session.sql("SELECT DATE_TRUNC('DAY', EVENT_TIMESTAMP) AS DAY, COUNT(*) AS EVENTS, SUM(CASE WHEN IS_CRITICAL THEN 1 ELSE 0 END) AS CRITICAL FROM TRANSFORM_DB.TRANSFORM_SCHEMA.CLEAN_ICU_EVENTS GROUP BY DAY ORDER BY DAY").to_pandas()
    line = alt.Chart(icu_trend).mark_line(color="#00B894").encode(x="DAY:T", y="EVENTS:Q")
    crit_line = alt.Chart(icu_trend).mark_line(color="#E74C3C", strokeDash=[5,3]).encode(x="DAY:T", y="CRITICAL:Q")
    st.altair_chart((line + crit_line).properties(height=250), use_container_width=True)

with tab4:
    st.header("Billing Analytics")
    b1, b2, b3 = st.columns(3)
    bill_stats = session.sql("SELECT SUM(TOTAL_AMOUNT) AS TOTAL, SUM(PAID_AMOUNT) AS PAID, SUM(OVERDUE_AMOUNT) AS OVERDUE FROM ANALYTICS_DB.ANALYTICS_SCHEMA.BILLING_ANALYTICS").collect()[0]
    b1.metric("Total Revenue", f"${bill_stats['TOTAL']:,.0f}")
    b2.metric("Paid", f"${bill_stats['PAID']:,.0f}")
    b3.metric("Overdue", f"${bill_stats['OVERDUE']:,.0f}")

    st.subheader("Billing Status Distribution")
    status_df = session.sql("SELECT STATUS, COUNT(*) AS CNT, SUM(AMOUNT) AS TOTAL_AMT FROM RAW_DB.RAW_SCHEMA.BILLING_DATA GROUP BY STATUS").to_pandas()
    st.altair_chart(alt.Chart(status_df).mark_arc(innerRadius=50).encode(theta="CNT:Q", color=alt.Color("STATUS:N", scale=alt.Scale(domain=["PAID","PENDING","OVERDUE"], range=["#00B894","#FDCB6E","#E74C3C"])), tooltip=["STATUS","CNT","TOTAL_AMT"]).properties(height=300), use_container_width=True)

    st.subheader("Monthly Revenue Trend")
    rev_df = session.sql("SELECT DATE_TRUNC('MONTH', BILL_DATE) AS MONTH, SUM(AMOUNT) AS REVENUE FROM RAW_DB.RAW_SCHEMA.BILLING_DATA GROUP BY MONTH ORDER BY MONTH").to_pandas()
    st.altair_chart(alt.Chart(rev_df).mark_bar(color="#FDCB6E").encode(x="MONTH:T", y="REVENUE:Q").properties(height=250), use_container_width=True)

with tab5:
    st.header("Rx Decisions (Marketplace Data)")
    st.caption("Source: Definitive Healthcare - RxDecision Insights Prescription Therapy Decisions")
    rx_df = session.sql("SELECT CLAIM_YEAR, MEDICATION_NAME, RX_EVENT_TYPE, RX_EVENT_CLAIMS, RX_TOTAL_CLAIMS, RX_EVENT_SCORE_DECILE FROM RAW_DB.RAW_SCHEMA.PRESCRIPTION_DATA ORDER BY CLAIM_YEAR DESC, RX_TOTAL_CLAIMS DESC").to_pandas()
    st.dataframe(rx_df, use_container_width=True, height=300)

    st.subheader("Rx Event Type Distribution")
    rx_evt = session.sql("SELECT RX_EVENT_TYPE, COUNT(*) AS CNT FROM RAW_DB.RAW_SCHEMA.PRESCRIPTION_DATA GROUP BY RX_EVENT_TYPE ORDER BY CNT DESC").to_pandas()
    st.altair_chart(alt.Chart(rx_evt).mark_bar(color="#A29BFE").encode(x=alt.X("CNT:Q", title="Count"), y=alt.Y("RX_EVENT_TYPE:N", sort="-x")).properties(height=250), use_container_width=True)

    st.subheader("Top Medications by Prescription Volume")
    med_df = session.sql("SELECT MEDICATION_NAME, COUNT(*) AS PRESCRIPTIONS FROM RAW_DB.RAW_SCHEMA.MEDICATION_RECORDS GROUP BY MEDICATION_NAME ORDER BY PRESCRIPTIONS DESC").to_pandas()
    st.altair_chart(alt.Chart(med_df).mark_bar(color="#FD79A8").encode(x="PRESCRIPTIONS:Q", y=alt.Y("MEDICATION_NAME:N", sort="-x")).properties(height=300), use_container_width=True)

with tab6:
    st.header("Hospitals & Providers (Marketplace Data)")
    st.caption("Source: Definitive Healthcare - HCO/HCP Reference Data")
    h1, h2, h3 = st.columns(3)
    h1.metric("Hospitals", session.sql("SELECT COUNT(*) AS CNT FROM RAW_DB.RAW_SCHEMA.HOSPITALS").collect()[0]["CNT"])
    h2.metric("HCO Locations", session.sql("SELECT COUNT(*) AS CNT FROM RAW_DB.RAW_SCHEMA.HCO_LOCATIONS").collect()[0]["CNT"])
    h3.metric("Provider Affiliations", session.sql("SELECT COUNT(*) AS CNT FROM RAW_DB.RAW_SCHEMA.PROVIDER_AFFILIATIONS").collect()[0]["CNT"])

    st.subheader("Hospital Details")
    hosp_df = session.sql("SELECT HOSPITAL_NAME, CITY, STATE, NUMBER_BEDS, NET_PATIENT_REVENUE, NET_INCOME, FINANCIAL_YEAR FROM RAW_DB.RAW_SCHEMA.HOSPITALS ORDER BY NET_PATIENT_REVENUE DESC NULLS LAST").to_pandas()
    st.dataframe(hosp_df, use_container_width=True, height=300)

    st.subheader("HCO Locations by Region")
    loc_df = session.sql("SELECT STATE, COUNT(*) AS LOCATIONS FROM RAW_DB.RAW_SCHEMA.HCO_LOCATIONS WHERE STATE IS NOT NULL GROUP BY STATE ORDER BY LOCATIONS DESC").to_pandas()
    if not loc_df.empty:
        st.altair_chart(alt.Chart(loc_df).mark_bar(color="#00CEC9").encode(x="LOCATIONS:Q", y=alt.Y("STATE:N", sort="-x")).properties(height=200), use_container_width=True)

with tab7:
    st.header("AI Risk Scoring (Feature Store)")
    r1, r2, r3 = st.columns(3)
    risk_stats = session.sql("SELECT SUM(CASE WHEN RISK_SCORE='HIGH' THEN 1 ELSE 0 END) AS HIGH_RISK, SUM(CASE WHEN RISK_SCORE='MEDIUM' THEN 1 ELSE 0 END) AS MED_RISK, SUM(CASE WHEN RISK_SCORE='LOW' THEN 1 ELSE 0 END) AS LOW_RISK FROM AI_READY_DB.FEATURE_STORE.ICU_FEATURE_STORE").collect()[0]
    r1.metric("High Risk", risk_stats["HIGH_RISK"], delta_color="inverse")
    r2.metric("Medium Risk", risk_stats["MED_RISK"])
    r3.metric("Low Risk", risk_stats["LOW_RISK"])

    st.subheader("Risk Distribution by Diagnosis")
    risk_df = session.sql("SELECT DIAGNOSIS, RISK_SCORE, COUNT(*) AS CNT FROM AI_READY_DB.FEATURE_STORE.ICU_FEATURE_STORE GROUP BY DIAGNOSIS, RISK_SCORE ORDER BY DIAGNOSIS").to_pandas()
    st.altair_chart(alt.Chart(risk_df).mark_bar().encode(x="DIAGNOSIS:N", y="CNT:Q", color=alt.Color("RISK_SCORE:N", scale=alt.Scale(domain=["HIGH","MEDIUM","LOW"], range=["#E74C3C","#FDCB6E","#00B894"]))).properties(height=300), use_container_width=True)

    st.subheader("Top High-Risk Patients")
    high_risk_df = session.sql("SELECT PATIENT_ID, AGE, GENDER, DIAGNOSIS, ROUND(AVG_HEART_RATE,1) AS AVG_HR, ROUND(AVG_OXYGEN,1) AS AVG_O2, CRITICAL_EVENTS, TOTAL_EVENTS, CRITICAL_EVENT_RATE, RISK_SCORE FROM AI_READY_DB.FEATURE_STORE.ICU_FEATURE_STORE WHERE RISK_SCORE = 'HIGH' ORDER BY CRITICAL_EVENT_RATE DESC LIMIT 20").to_pandas()
    st.dataframe(high_risk_df, use_container_width=True, height=350)

st.divider()
st.caption("Healthcare RxDecision Analytics Platform | Data: Snowflake Marketplace (Definitive Healthcare) | Account: uub65990 | Author: DEVIKAPG")
