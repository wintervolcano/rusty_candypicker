use anyhow::{anyhow, Result};
use clap::{Arg, Command};
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs;
use xmltree::{Element, EmitterConfig};

const SPEED_OF_LIGHT: f64 = 299_792_458.0;

#[derive(Debug, Clone)]
struct Candidate {
    snr: f64,
    period: f64,
    f0: f64,
    dm: f64,
    acc: f64,
    nh: i32,
    ddm_count_ratio: f32,
    ddm_snr_ratio: f32,
    nassoc: i32,
    period_ms: i32,
    #[allow(dead_code)]
    pulse_width: f64,
    uuid: Option<String>,
    xml_file: String,
    candidate_id: i32,
    raw_xml: String,
    related: Vec<usize>,
    is_pivot: bool,
}

impl Candidate {
    fn new(
        snr: f64,
        period: f64,
        dm: f64,
        acc: f64,
        nh: i32,
        ddm_count_ratio: f32,
        ddm_snr_ratio: f32,
        nassoc: i32,
        uuid: Option<String>,
        xml_file: String,
        candidate_id: i32,
        raw_xml: String,
    ) -> Self {
        let f0 = 1.0 / period;
        let period_ms = (period * 1000.0).round() as i32;
        let pulse_width = period / 2.0f64.powi(nh);
        Self {
            snr,
            period,
            f0,
            dm,
            acc,
            nh,
            ddm_count_ratio,
            ddm_snr_ratio,
            nassoc,
            period_ms,
            pulse_width,
            uuid,
            xml_file,
            candidate_id,
            raw_xml,
            related: Vec::new(),
            is_pivot: false,
        }
    }

    fn is_related(&self, other: &Candidate, period_thresh: f64, dm_thresh: Option<f64>, tobs_over_c: f64) -> bool {
        if let Some(dmth) = dm_thresh {
            if (self.dm - other.dm).abs() > dmth {
                return false;
            }
        }
        let corrected_other_period =
            1.0 / (other.f0 - (other.acc - self.acc) * other.f0 * tobs_over_c);
        let true_period_difference = if (self.period / corrected_other_period) > 1.0 {
            self.period % corrected_other_period
        } else {
            corrected_other_period % self.period
        };
        true_period_difference <= period_thresh
            || (self.period - corrected_other_period).abs() <= period_thresh
    }
}

#[derive(Debug)]
struct XmlSections {
    misc_info: Option<String>,
    header_parameters: Option<String>,
    search_parameters: Option<String>,
    segment_parameters: Option<String>,
    dedispersion_trials: Option<String>,
    acceleration_trials: Option<String>,
    cuda_device_parameters: Option<String>,
    execution_times: Option<String>,
}

#[derive(Debug)]
struct XmlFile {
    filename: String,
    sections: XmlSections,
    fft_size: i64,
    tsamp: f64,
    candidates: Vec<Candidate>,
}

fn element_to_string(e: &Element) -> String {
    let mut buf = Vec::new();
    e.write_with_config(&mut buf, EmitterConfig::new().perform_indent(true))
        .expect("serialize element");
    String::from_utf8(buf).unwrap()
}

fn slice_candidate_block(xml: &str, id: i32) -> Option<String> {
    let pat = format!("<candidate id='{id}'>");
    if let Some(start) = xml.find(&pat) {
        if let Some(end) = xml[start..].find("</candidate>") {
            let block = &xml[start..start + end + "</candidate>".len()];
            return Some(block.to_string());
        }
    }
    None
}

fn get_text_path(root: &Element, path: &[&str]) -> Option<String> {
    let mut cur = root;
    for &p in path {
        cur = cur.get_child(p)?;
    }
    cur.get_text().map(|cow| cow.to_string())
}

fn get_text_child(el: &Element, tag: &str, filename: &str) -> Result<String> {
    el.get_child(tag)
        .and_then(|e| e.get_text().map(|cow| cow.to_string()))
        .ok_or_else(|| anyhow!("Missing <{}> in {}", tag, filename))
}

fn parse_xml_file(filename: &str) -> Result<XmlFile> {
    println!("[INFO] Parsing {filename}");
    let content = fs::read_to_string(filename)?;
    let root: Element = Element::parse(content.as_bytes())?;

    let tsamp: f64 = get_text_path(&root, &["header_parameters", "tsamp"])
        .ok_or_else(|| anyhow!("Missing tsamp in {}", filename))?
        .parse()?;

    let fft_size: i64 = get_text_path(&root, &["search_parameters", "size"])
        .ok_or_else(|| anyhow!("Missing fft size in {}", filename))?
        .parse()?;

    let mut candidates = Vec::new();
    if let Some(cands_el) = root.get_child("candidates") {
        for cand_el in &cands_el.children {
            if let xmltree::XMLNode::Element(e) = cand_el {
                let cid = e.attributes.get("id")
                    .ok_or_else(|| anyhow!("Candidate missing id in {}", filename))?
                    .parse::<i32>()?;
                let period: f64 = get_text_child(e, "period", filename)?.parse()?;
                let dm: f64 = get_text_child(e, "dm", filename)?.parse()?;
                let acc: f64 = get_text_child(e, "acc", filename)?.parse()?;
                let nh: i32 = get_text_child(e, "nh", filename)?.parse()?;
                let snr: f64 = get_text_child(e, "snr", filename)?.parse()?;
                let ddm_count_ratio: f32 = get_text_child(e, "ddm_count_ratio", filename)?.parse()?;
                let ddm_snr_ratio: f32 = get_text_child(e, "ddm_snr_ratio", filename)?.parse()?;
                let nassoc: i32 = get_text_child(e, "nassoc", filename)?.parse()?;
                let uuid = get_text_path(e, &["search_candidates_database_uuid"]);
                let raw_xml = slice_candidate_block(&content, cid).unwrap_or_else(|| element_to_string(e));
                candidates.push(Candidate::new(
                    snr, period, dm, acc, nh,
                    ddm_count_ratio, ddm_snr_ratio, nassoc,
                    uuid, filename.to_string(), cid, raw_xml,
                ));
            }
        }
    }

    let sections = XmlSections {
        misc_info: root.get_child("misc_info").map(element_to_string),
        header_parameters: root.get_child("header_parameters").map(element_to_string),
        search_parameters: root.get_child("search_parameters").map(element_to_string),
        segment_parameters: root.get_child("segment_parameters").map(element_to_string),
        dedispersion_trials: root.get_child("dedispersion_trials").map(element_to_string),
        acceleration_trials: root.get_child("acceleration_trials").map(element_to_string),
        cuda_device_parameters: root.get_child("cuda_device_parameters").map(element_to_string),
        execution_times: root.get_child("execution_times").map(element_to_string),
    };

    println!("[INFO] Parsed {filename}: {} candidates", candidates.len());
    Ok(XmlFile { filename: filename.to_string(), sections, fft_size, tsamp, candidates })
}

fn cluster_candidates(cands: &mut [Candidate], period_thresh: f64, dm_thresh: Option<f64>, tobs_over_c: f64, bin_dm: bool) {
    println!("[INFO] Clustering (binning: {bin_dm})...");
    let n = cands.len();
    if bin_dm {
        let mut bins: HashMap<i64, Vec<usize>> = HashMap::new();
        for (i, c) in cands.iter().enumerate() {
            let b = if let Some(dmth) = dm_thresh {
                (c.dm / dmth).floor() as i64
            } else {
                0
            };
            bins.entry(b).or_default().push(i);
        }
        let results: Vec<(usize, Vec<usize>)> = bins.into_par_iter().flat_map(|(_, idxs)| {
            idxs.iter().map(|&i| {
                let mut rels = Vec::new();
                for &j in &idxs {
                    if j > i && cands[i].is_related(&cands[j], period_thresh, dm_thresh, tobs_over_c) {
                        rels.push(j);
                    }
                }
                (i, rels)
            }).collect::<Vec<_>>()
        }).collect();
        for (i, rels) in results {
            cands[i].related = rels;
        }
    } else {
        let results: Vec<(usize, Vec<usize>)> = (0..n).into_par_iter().map(|i| {
            let mut rels = Vec::new();
            for j in (i+1)..n {
                if cands[i].is_related(&cands[j], period_thresh, dm_thresh, tobs_over_c) {
                    rels.push(j);
                }
            }
            (i, rels)
        }).collect();
        for (i, rels) in results {
            cands[i].related = rels;
        }
    }
    println!("[INFO] Finished clustering.");
}

fn shortlist_candidates(cands: &mut [Candidate]) -> Vec<usize> {
    println!("[INFO] Shortlisting pivots...");
    let mut to_remove: HashSet<usize> = HashSet::new();
    for i in 0..cands.len() {
        if cands[i].related.len() > 1 {
            for &r in &cands[i].related {
                to_remove.insert(r);
            }
        }
    }
    let mut pivots = Vec::new();
    for (i, c) in cands.iter_mut().enumerate() {
        if !to_remove.contains(&i) {
            c.is_pivot = true;
            pivots.push(i);
        }
    }
    println!("[INFO] Found {} pivots.", pivots.len());
    pivots
}

fn save_candidates_csv(cands: &[Candidate], pivots: &[usize], filename: &str) -> Result<()> {
    println!("[INFO] Writing {filename}");
    let mut wtr = csv::Writer::from_path(filename)?;
    wtr.write_record(&["snr","period","dm","acc","nh","ddm_count_ratio","ddm_snr_ratio","nassoc",
        "period_ms","uuid","xml_file","candidate_id","num_related","related_cands"])?;
    for &i in pivots {
        let c = &cands[i];
        let related_ids: Vec<String> = c.related.iter().map(|&j| {
            cands[j].uuid.clone().unwrap_or_else(|| format!("{}_{}", cands[j].xml_file, cands[j].candidate_id))
        }).collect();
        wtr.write_record(&[
            c.snr.to_string(),
            format!("{:.17}", c.period),
            format!("{:.8}", c.dm),
            c.acc.to_string(),
            c.nh.to_string(),
            c.ddm_count_ratio.to_string(),
            c.ddm_snr_ratio.to_string(),
            c.nassoc.to_string(),
            c.period_ms.to_string(),
            c.uuid.clone().unwrap_or_default(),
            c.xml_file.clone(),
            c.candidate_id.to_string(),
            c.related.len().to_string(),
            related_ids.join(":"),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn strip_xml_decl(s: &str) -> &str {
    // Remove any UTF-8 BOM and leading whitespace
    let trimmed = s.trim_start_matches(|c: char| c == '\u{feff}' || c.is_whitespace());    // If it starts with an XML declaration, skip it
    if trimmed.starts_with("<?xml") {
        // find the end of declaration "?>"
        if let Some(pos) = trimmed.find("?>") {
            return trimmed[(pos + 2)..].trim_start();
        }
    }
    trimmed
}

fn write_updated_xmls(
    xf: &XmlFile,
    _cands: &[Candidate],
    pivot_map: &HashMap<(String, i32), bool>,
) -> Result<()> {
    use std::io::BufRead;

    // --- Preserve the original XML declaration from the first line ---
    let file = fs::File::open(&xf.filename)?;
    let mut first_line = String::new();
    {
        let mut reader = std::io::BufReader::new(&file);
        reader.read_line(&mut first_line)?;
    }
    let xml_decl = if first_line.trim_start().starts_with("<?xml") {
        first_line.trim().to_string()
    } else {
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>".to_string()
    };

    let picked_name = xf.filename.replace(".xml", "_picked.xml");
    let rejected_name = xf.filename.replace(".xml", "_rejected.xml");

    let mut base = String::new();
    base.push_str(&xml_decl);
    base.push('\n');
    base.push_str("<peasoup_search>\n");

    if let Some(s) = &xf.sections.misc_info { base.push_str(strip_xml_decl(s)); base.push('\n'); }
    if let Some(s) = &xf.sections.header_parameters { base.push_str(strip_xml_decl(s)); base.push('\n'); }
    if let Some(s) = &xf.sections.search_parameters { base.push_str(strip_xml_decl(s)); base.push('\n'); }
    if let Some(s) = &xf.sections.segment_parameters { base.push_str(strip_xml_decl(s)); base.push('\n'); }
    if let Some(s) = &xf.sections.dedispersion_trials { base.push_str(strip_xml_decl(s)); base.push('\n'); }
    if let Some(s) = &xf.sections.acceleration_trials { base.push_str(strip_xml_decl(s)); base.push('\n'); }
    if let Some(s) = &xf.sections.cuda_device_parameters { base.push_str(strip_xml_decl(s)); base.push('\n'); }

    let mut picked = base.clone();
    let mut rejected = base.clone();
    picked.push_str("<candidates>\n");
    rejected.push_str("<candidates>\n");

    for c in &xf.candidates {
        let is_pivot = *pivot_map.get(&(c.xml_file.clone(), c.candidate_id)).unwrap_or(&false);
        if is_pivot {
            picked.push_str(strip_xml_decl(&c.raw_xml));
            picked.push('\n');
        } else {
            rejected.push_str(strip_xml_decl(&c.raw_xml));
            rejected.push('\n');
        }
    }

    picked.push_str("</candidates>\n");
    rejected.push_str("</candidates>\n");

    if let Some(s) = &xf.sections.execution_times {
        picked.push_str(strip_xml_decl(s));
        picked.push('\n');
        rejected.push_str(strip_xml_decl(s));
        rejected.push('\n');
    }

    picked.push_str("</peasoup_search>\n");
    rejected.push_str("</peasoup_search>\n");

    fs::write(&picked_name, picked)?;
    fs::write(&rejected_name, rejected)?;
    println!("[INFO] Wrote {picked_name} and {rejected_name}");

    Ok(())
}

fn main() -> Result<()> {
    let matches = Command::new("candy_picker_rs")
        .version("0.3.0")
        .arg(Arg::new("period_thresh").short('p').num_args(1).required(true))
        .arg(Arg::new("dm_thresh").short('d').num_args(1))
        .arg(Arg::new("ncpus").short('n').num_args(1).default_value("8"))
        .arg(Arg::new("bin_dm").long("bin-dm").action(clap::ArgAction::SetTrue))
        .arg(Arg::new("xml_files").num_args(1..).required(true))
        .get_matches();

    let period_thresh: f64 = matches.get_one::<String>("period_thresh").unwrap().parse()?;
    let dm_thresh: Option<f64> = matches.get_one::<String>("dm_thresh").map(|s| s.parse().unwrap());
    let ncpus: usize = matches.get_one::<String>("ncpus").unwrap().parse()?;
    let bin_dm: bool = matches.get_flag("bin_dm");
    let xml_files: Vec<String> = matches.get_many::<String>("xml_files").unwrap().map(|s| s.to_string()).collect();

    println!("[INFO] Settings: period_thresh={period_thresh}, dm_thresh={:?}, workers={ncpus}, bin_dm={bin_dm}", dm_thresh);
    rayon::ThreadPoolBuilder::new().num_threads(ncpus).build_global().unwrap();

    let mut xml_file_objects = Vec::new();
    let mut all_candidates = Vec::new();
    for f in &xml_files {
        let xf = parse_xml_file(f)?;
        all_candidates.extend(xf.candidates.clone());
        xml_file_objects.push(xf);
    }
    if all_candidates.is_empty() {
        return Err(anyhow!("No candidates found"));
    }
    if xml_file_objects.len() > 1 {
        for xf in &xml_file_objects[1..] {
            if xf.fft_size != xml_file_objects[0].fft_size || xf.tsamp != xml_file_objects[0].tsamp {
                return Err(anyhow!("fft size and tsamp differ across files"));
            }
        }
    }
    let effective_tobs = xml_file_objects[0].fft_size as f64 * xml_file_objects[0].tsamp;
    let tobs_over_c = effective_tobs / SPEED_OF_LIGHT;
    println!("[INFO] Effective TOBS: {effective_tobs} s");

    cluster_candidates(&mut all_candidates, period_thresh, dm_thresh, tobs_over_c, bin_dm);
    let pivots = shortlist_candidates(&mut all_candidates);
    save_candidates_csv(&all_candidates, &pivots, "pivots.csv")?;

    let mut pivot_map: HashMap<(String,i32), bool> = HashMap::new();
    for &i in &pivots {
        pivot_map.insert((all_candidates[i].xml_file.clone(), all_candidates[i].candidate_id), true);
    }
    for xf in &xml_file_objects {
        write_updated_xmls(xf, &all_candidates, &pivot_map)?;
    }
    println!("[INFO] All done.");
    Ok(())
}
