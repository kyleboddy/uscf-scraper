#!/usr/bin/env python3
"""
Two-pass row-by-row parser for USCF MSA "Games" table, with debug logs.

Enhanced to ensure your own pre/post rating is parsed from the "Rating" row
in the player-specific page. We remove filler <td> if present in that row,
then parse e.g. "R: 1294 ->1451".

We also:
 - Remove "R: " from opponent rating columns
 - Extract opponent ID from name if in parentheses
 - Truncate location from "CITY, ST  ZIP" => "CITY, ST"
"""

import re
import sys
import time
import csv
import logging
import argparse
import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
from datetime import datetime
import matplotlib.pyplot as plt

# Only needed if using summarization
import openai

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG,  # keep DEBUG
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

BASE_URL = "https://www.uschess.org/msa/"
TOURN_HISTORY_URL = BASE_URL + "MbrDtlTnmtHst.php?{player_id}"

def fetch_html(url: str, timeout_sec=15, max_retries=3, sleep_sec=1.0) -> str:
    for attempt in range(1, max_retries + 1):
        logging.debug(f"fetch_html (attempt {attempt}/{max_retries}) => {url}")
        time.sleep(sleep_sec)
        try:
            resp = requests.get(url, timeout=timeout_sec)
            resp.raise_for_status()
            return resp.text
        except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout) as e:
            logging.debug(f"Timeout: {e}")
            if attempt == max_retries:
                raise
        except requests.exceptions.RequestException as e:
            logging.debug(f"Request error: {e}")
            if attempt == max_retries:
                raise
    return ""

def parse_date_prefix(txt: str):
    m = re.match(r"(\d{4}-\d{2}-\d{2})", txt)
    return m.group(1) if m else txt

def fix_location(raw_loc: str)->str:
    """
    If "LAS VEGAS, NV  89103" => "LAS VEGAS, NV"
    """
    s = re.sub(r"\s+", " ", raw_loc).strip()
    m = re.match(r"^(.+,\s*\w\w)\b", s)
    if m:
        return m.group(1)
    return s

def parse_rating_pair(txt: str):
    """
    e.g. "1294 =>1451" => ("1294","1451")
    """
    if "=>" in txt:
        left, right = txt.split("=>", 1)
        return left.strip(), right.strip()
    return txt.strip(), ""

def parse_tournament_list(html: str)->list:
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr", bgcolor=lambda x: x in ("FFFFC0", "FFFF80"))
    out=[]
    for r in rows:
        tds = r.find_all("td")
        if len(tds)<5:
            continue
        date_col = tds[0].get_text(strip=True)
        end_date = date_col.split()[0] if date_col else ""
        me = re.search(r"(\d{9,12})", date_col)
        event_id = me.group(1) if me else ""

        link_ = tds[1].find("a", href=True)
        event_name = tds[1].get_text(strip=True)
        xlink = link_["href"] if link_ else ""

        rb, ra = parse_rating_pair(tds[2].get_text(strip=True))
        qb, qa = parse_rating_pair(tds[3].get_text(strip=True))
        bb, ba = parse_rating_pair(tds[4].get_text(strip=True))

        out.append({
            "end_date": end_date,
            "event_id": event_id,
            "event_name": event_name,
            "reg_before": rb,
            "reg_after": ra,
            "quick_before": qb,
            "quick_after": qa,
            "blitz_before": bb,
            "blitz_after": ba,
            "xlink": xlink,
        })
    return out

def find_all_sections_link(soup: BeautifulSoup, xlink: str)->str:
    m = re.search(r"XtblMain\.php\?(\d+)-(\d+)", xlink)
    if not m:
        return ""
    eid, pid = m.groups()
    zero = f"XtblMain.php?{eid}.0-{pid}"
    link_ = soup.find("a", href=zero)
    if link_:
        return link_["href"]
    return zero

def parse_summary_table(soup: BeautifulSoup)->dict:
    """
    We parse "location", "event date(s)", "chief td", etc.
    But the player's rating row is often in a different table 
    (the 'player-specific' table that includes "Rating", "Score", etc.).
    So we do NOT expect to see "Rating" in this function for that big event table.
    """
    data={}
    big = soup.find("table", {"bgcolor":"FFFFFF","width":"750"})
    if not big:
        big = soup.find("table", {"bgcolor":"FFFFFF","width":"800"})
    if not big:
        return data
    rows = big.find_all("tr")
    for r in rows:
        tds = r.find_all("td")
        if len(tds)<2:
            continue
        lbl = tds[0].get_text(strip=True).lower()
        val = tds[1].get_text(" ", strip=True)
        # fix location if present
        if lbl=="location":
            val = fix_location(val)
        data[lbl] = val
    return data

def parse_games_in_table(games_table: BeautifulSoup, pass_label: str)->list:
    all_tr = games_table.find_all("tr", recursive=True)
    logging.debug(f"{pass_label}: Found {len(all_tr)} <tr> in 'Games' table.")
    out=[]
    for tr_idx,tr in enumerate(all_tr):
        tds = tr.find_all("td", recursive=False)
        if not tds:
            continue
        # remove filler
        new_cells=[]
        for td_ in tds:
            open_td = re.search(r"<td[^>]*>", str(td_), re.IGNORECASE)
            if open_td:
                td_tag = open_td.group(0)
                if re.search(r'width\s*=\s*["\']?1["\']?', td_tag, re.IGNORECASE) and \
                   re.search(r'rowspan\s*=\s*["\']?20["\']?', td_tag, re.IGNORECASE):
                    logging.debug(f"{pass_label}: skipping filler => {td_tag}")
                    continue
            new_cells.append(td_)

        # parse text
        text_cols=[]
        for c_ in new_cells:
            c2 = re.sub(r"<[^>]*>", "", str(c_), flags=re.IGNORECASE|re.DOTALL)
            c2 = c2.replace("\xa0"," ")
            c2 = re.sub(r"\s+", " ", c2)
            c2 = c2.strip()
            text_cols.append(c2)

        if len(text_cols)==6:
            # check col0 => ^[WLDH]
            if re.match(r"^[WLDH]\s*\d*$", text_cols[0], re.IGNORECASE):
                # remove "R:" from opp rating
                opp_pre = re.sub(r"^R:\s*", "", text_cols[3], flags=re.IGNORECASE)
                opp_post= re.sub(r"^R:\s*", "", text_cols[4], flags=re.IGNORECASE)
                opp_name= text_cols[5]
                opp_id=""
                # parse e.g. "MARK E FRASER (12476390)"
                mm = re.match(r"^(.*)\((\d+)\)", opp_name)
                if mm:
                    opp_name = mm.group(1).strip()
                    opp_id   = mm.group(2).strip()
                game={
                    "result": text_cols[0].split()[0],
                    "color":  text_cols[1],
                    "opp_score": text_cols[2],
                    "opp_pre": opp_pre,
                    "opp_post":opp_post,
                    "opp_name":opp_name,
                    "opp_id":  opp_id,
                }
                logging.debug(f"{pass_label}: Row {tr_idx} => Found game => {game}")
                out.append(game)
    return out

def parse_player_rating_table(soup: BeautifulSoup)->(str,str):
    """
    The "Rating" row is in a table with width=750, 
    specifically the one that has <td>Rating</td> in the first column.
    But there's filler <td width=1 rowspan=20>.
    We'll remove that filler, then parse "R: 1294 ->1451".
    Returns (my_rating_pre, my_rating_post).
    """
    pre, post = "", ""
    # find the table that has <td>Rating</td> somewhere
    table_list = soup.find_all("table", {"bgcolor":"FFFFFF","width":"750"})
    # We check each until we find a row whose first cell is "Rating"
    for t_ in table_list:
        # gather <tr>
        rows = t_.find_all("tr", recursive=False)
        for r_ in rows:
            tds = r_.find_all("td", recursive=False)
            if not tds:
                continue
            # remove filler
            cleaned=[]
            for td_ in tds:
                open_td = re.search(r"<td[^>]*>", str(td_), re.IGNORECASE)
                if open_td:
                    td_tag = open_td.group(0)
                    if re.search(r'width\s*=\s*["\']?1["\']?', td_tag, re.IGNORECASE) and \
                       re.search(r'rowspan\s*=\s*["\']?20["\']?', td_tag, re.IGNORECASE):
                        logging.debug(f"parse_player_rating_table => removing filler => {td_tag}")
                        continue
                cleaned.append(td_)

            if len(cleaned)>=2:
                lbl = cleaned[0].get_text(strip=True).lower()
                val = cleaned[1].get_text(" ", strip=True)
                if "rating" == lbl:
                    # parse "R: 1294 ->1451" or "R: 1294 =>1451"
                    rating_line = val.strip()
                    mrat = re.search(r"R:\s*(\d+)\s*(?:->|=>)\s*(\d+)", rating_line, re.IGNORECASE)
                    if mrat:
                        pre, post = mrat.group(1), mrat.group(2)
                        logging.debug(f"Found user rating => pre={pre}, post={post}")
                        return pre, post
    return pre, post

def parse_player_specific_page(html: str)->dict:
    """
    1) We parse the rating from the "Rating" row (in the table that has width=750 
       but is the 'player-specific' table).
    2) Then we do our two-pass "Games" parse from the <b>Games</b> table.
    """
    ret = {"player_pre_rating":"","player_post_rating":"","games":[]}
    soup = BeautifulSoup(html, "html.parser")

    # parse rating from the 'player rating table'
    mypre, mypost = parse_player_rating_table(soup)
    ret["player_pre_rating"]= mypre
    ret["player_post_rating"]=mypost

    # find "Games" table
    all_t = soup.find_all("table")
    games_table=None
    for t_ in all_t:
        if t_.find("b", string=re.compile("Games", re.IGNORECASE)):
            games_table= t_
            break
    if not games_table:
        logging.debug("No <b>Games</b> found => no data.")
        return ret

    # pass1
    pass1_g= parse_games_in_table(games_table, "Pass1")
    if pass1_g:
        ret["games"] = pass1_g
        return ret
    # pass2
    logging.debug("Pass1 => 0 games => checking nested <table> pass2.")
    sub_tab = games_table.find_all("table", recursive=True)
    pass2_c=[]
    for i, st in enumerate(sub_tab):
        st_g = parse_games_in_table(st, f"Pass2(subtable={i})")
        pass2_c.extend(st_g)
    if pass2_c:
        ret["games"]= pass2_c
        return ret

    # still 0 => dump
    logging.debug("\n\n===== DUMP 'GAMES' TABLE HTML =====\n")
    logging.debug(str(games_table))
    logging.warning("0 games found after 2-pass parse => see debug above.")
    return ret

def parse_crosstable(xlink: str, player_id: str, visited: set,
                     timeout_sec=15, max_retries=3) -> dict:
    data={"summary":{}, "player_games":[],"player_rating_pre":"","player_rating_post":""}
    if xlink in visited:
        return data
    visited.add(xlink)

    full_url= BASE_URL + xlink
    logging.debug(f"parse_crosstable => {full_url}")
    raw= fetch_html(full_url, timeout_sec=timeout_sec, max_retries=max_retries)
    soup= BeautifulSoup(raw,"html.parser")
    data["summary"]= parse_summary_table(soup)

    # find user link
    userlink=""
    for a_tag in soup.find_all("a", href=True):
        if "XtblPlr.php?" in a_tag["href"] and str(player_id) in a_tag["href"]:
            userlink=a_tag["href"]
            logging.debug(f"Found user link => {userlink}")
            break

    if not userlink:
        # try .0
        zero= find_all_sections_link(soup, xlink)
        if zero and zero not in visited:
            visited.add(zero)
            logging.info(f"Trying all-sections => {zero}")
            all_html= fetch_html(BASE_URL+zero, timeout_sec=timeout_sec, max_retries=max_retries)
            soup2= BeautifulSoup(all_html,"html.parser")
            summ2= parse_summary_table(soup2)
            for k,v in summ2.items():
                data["summary"][k]=v
            # find user link again
            for a2 in soup2.find_all("a", href=True):
                if "XtblPlr.php?" in a2["href"] and str(player_id) in a2["href"]:
                    userlink=a2["href"]
                    logging.debug(f"Found user link in .0 => {userlink}")
                    break

    if userlink:
        plr_html= fetch_html(BASE_URL+userlink, timeout_sec=timeout_sec, max_retries=max_retries)
        parsed= parse_player_specific_page(plr_html)
        data["player_games"] = parsed["games"]
        data["player_rating_pre"]= parsed["player_pre_rating"]
        data["player_rating_post"]= parsed["player_post_rating"]
    else:
        logging.debug("No user link => 0 games.")
    return data

def main(player_id, filter_year=None, openai_prompt=None,
         do_graph=False, timeout_sec=15, max_retries=3):
    main_url= TOURN_HISTORY_URL.format(player_id=player_id)
    main_html= fetch_html(main_url, timeout_sec=timeout_sec, max_retries=max_retries)
    from bs4 import BeautifulSoup
    events= parse_tournament_list(main_html)
    if filter_year:
        events=[e for e in events if e["end_date"].startswith(filter_year)]
    logging.debug(f"After filtering => {len(events)} events")

    visited=set()
    all_data=[]
    per_game=[]
    rating_data=[]
    for i,ev in enumerate(events,1):
        logging.info(f"Parsing event {i}/{len(events)} => {ev['event_name']} (ID={ev['event_id']})")
        row={
            "player_id": player_id,
            "end_date": ev["end_date"],
            "event_id": ev["event_id"],
            "event_name": ev["event_name"],
            "reg_before": ev["reg_before"],
            "reg_after": ev["reg_after"],
            "quick_before": ev["quick_before"],
            "quick_after": ev["quick_after"],
            "blitz_before": ev["blitz_before"],
            "blitz_after": ev["blitz_after"],
            "location":"",
            "event_date(s)":"",
            "chief_td":"",
        }
        dpre= parse_date_prefix(ev["end_date"])
        dt=None
        try:
            dt= datetime.strptime(dpre,"%Y-%m-%d")
        except: pass
        rt=None
        try:
            ra_clean= re.sub(r"[^0-9]+", "", ev["reg_after"])
            if ra_clean:
                rt= int(ra_clean)
        except: pass
        if dt and rt is not None:
            rating_data.append((dt, rt))

        # parse cross
        cdata={}
        if ev["xlink"]:
            cdata= parse_crosstable(ev["xlink"], player_id, visited,
                                    timeout_sec=timeout_sec, max_retries=max_retries)

        row["location"]      = cdata.get("summary",{}).get("location","")
        row["event_date(s)"] = cdata.get("summary",{}).get("event date(s)","")
        row["chief_td"]      = cdata.get("summary",{}).get("chief td","")

        # user rating from player page
        my_pre = cdata.get("player_rating_pre","")
        my_post= cdata.get("player_rating_post","")
        if my_pre:  row["reg_before"]= my_pre
        if my_post: row["reg_after"] = my_post

        # gather games
        gms= cdata.get("player_games", [])
        logging.info(f"Event {ev['event_id']}: Found {len(gms)} game(s).")
        for gm in gms:
            per_game.append({
                "event_id": ev["event_id"],
                "event_name": ev["event_name"],
                "section_name":"",
                "round":"",
                "result": gm["result"],
                "color": gm["color"],
                "my_rating_pre": my_pre,
                "my_rating_post":my_post,
                "opp_id": gm["opp_id"],
                "opp_name": gm["opp_name"],
                # we remove "R: " from them in CSV stage
                "opp_rating_pre": gm["opp_pre"],
                "opp_rating_post":gm["opp_post"],
                "location": row["location"],
                "event_date(s)": row["event_date(s)"],
            })
        all_data.append(row)

    ts=int(time.time())
    out_csv= f"uscf_scraper_{ts}.csv"
    logging.debug(f"Saving CSV => {out_csv}")
    with open(out_csv,"w",newline="",encoding="utf-8") as f:
        w= csv.writer(f)
        w.writerow([
            "event_id","event_name","section_name","round","result","color",
            "my_rating_pre","my_rating_post","opp_id","opp_name",
            "opp_rating_pre","opp_rating_post","location","event_date(s)"
        ])
        for g in per_game:
            # remove "R:" from opp pre/post in CSV
            oppp= re.sub(r"^R:\s*", "", g["opp_rating_pre"], flags=re.IGNORECASE)
            oppo= re.sub(r"^R:\s*", "", g["opp_rating_post"], flags=re.IGNORECASE)
            w.writerow([
                g["event_id"], g["event_name"], g["section_name"], g["round"],
                g["result"], g["color"], g["my_rating_pre"], g["my_rating_post"],
                g["opp_id"], g["opp_name"],
                oppp, oppo,
                g["location"], g["event_date(s)"]
            ])

    # Summaries
    if openai_prompt and os.getenv("OPENAI_API_KEY"):
        logging.debug("Invoking OpenAI summarization.")
        lines=[]
        for gm in per_game[:200]:
            line=(f"EventID={gm['event_id']} | {gm['event_name']} | "
                  f"Result={gm['result']} | Color={gm['color']} | Opp={gm['opp_name']} "
                  f"(pre={gm['opp_rating_pre']} post={gm['opp_rating_post']})")
            lines.append(line)
        userp= f"{openai_prompt}\n\nHere is data:\n" + "\n".join(lines)
        try:
            comp= openai.Completion.create(
                model="text-davinci-003",
                prompt=userp,
                max_tokens=500,
                temperature=0.7
            )
            print("\n--- OPENAI SUMMARY ---\n")
            print(comp.choices[0].text.strip())
            print("\n--- END SUMMARY ---\n")
        except Exception as e:
            logging.debug(f"OpenAI error => {e}")

    # rating chart
    if do_graph and rating_data:
        rating_data.sort(key=lambda x:x[0])
        ds=[x[0] for x in rating_data]
        rs=[x[1] for x in rating_data]
        plt.figure(figsize=(10,6))
        plt.plot(ds, rs, marker='o', color='blue', label="USCF Rating")
        plt.title(f"Rating Over Time (Player {player_id})")
        plt.xlabel("Date")
        plt.ylabel("Rating")
        plt.grid(True)
        plt.legend()
        outpng= f"uscf_rating_plot_{player_id}_{ts}.png"
        plt.savefig(outpng, dpi=150)
        logging.debug(f"Saved rating chart => {outpng}")
        plt.close()

    logging.debug("\n=== FINAL EVENT SUMMARY ===\n")
    for row in all_data:
        logging.debug(row)


if __name__=="__main__":
    parser= argparse.ArgumentParser(
        description="Two-pass USCF MSA parser with user rating fix, no R: for opp, location fix, etc.")
    parser.add_argument("--player", required=True)
    parser.add_argument("--year", default=None)
    parser.add_argument("--openai-summarize", default=None)
    parser.add_argument("--graph", action="store_true")
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--max-retries", type=int, default=3)
    args= parser.parse_args()
    main(
        player_id=args.player,
        filter_year=args.year,
        openai_prompt=args.openai_summarize,
        do_graph=args.graph,
        timeout_sec=args.timeout,
        max_retries=args.max_retries
    )
