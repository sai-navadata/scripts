# Handover Docs

-  All scripts run on a digitalocean server which needs to be paid in duly manner else the entire VM gets deleted. Email and Password are available with Saumya/Mahima
- You can access this said VM from the digitalocean console. Ensure that you set the user to `sai`
- The scripts will be located in the `attendance` directory in home.
- You can check the active cron jobs by running `crontab -l ` in the terminal
- cron jobs are set in UTC and any changes in timings have to be translated from IST to UTC
- In case of ratelimits due to an increase in sheets, the script’s sleep timings need to be adjusted accordingly. Occasionally, the scripts might not work due to the rate limits in which case, you need to wait for a few hours to re-run them or create a new API key.



## Filenames:
Lrm duration - `/home/sai/attendance/lrm_dur.py`

Machiodes: 
 - Hindi -  `/home/sai/attendance/macchiodes_hindi.py`
 - Odia - `/home/sai/attendance/macchiodes_odia.py`
 - Gujrati - `/home/sai/attendance/macchiodes_gujrati.py`

LC report - `/home/sai/attendance/lc_report.py`

Grant management - `/home/sai/attendance/grant_management.py`

## How to run:
`/usr/bin/python3 <script_name>`

### PSA: Esure all python libraries are installed with `pip install -r requirements.txt` and add necessary api tokens and webhooks to .env




