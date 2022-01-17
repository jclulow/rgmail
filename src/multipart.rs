use anyhow::{bail, Result};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct Part {
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct Multipart {
    pub parts: Vec<Part>,
}

const DASHDASH: &[u8] = b"--";
const CRLF: &[u8] = b"\r\n";

pub fn multipart_parse(data: &[u8], boundary: &[u8]) -> Result<Multipart> {
    let mut mp = Multipart::default();
    let mut parts: Vec<Vec<u8>> = Vec::new();

    #[derive(Debug, PartialEq)]
    enum State {
        Rest,
        Part,
        PartOrEnd,
    }

    let bound = {
        let mut bound = DASHDASH.to_vec();
        for b in boundary {
            bound.push(*b);
        }
        bound
    };

    let start = {
        let mut start = bound.clone();
        for b in CRLF {
            start.push(*b);
        }
        start
    };

    let crlfstart = {
        let mut crlfstart = CRLF.to_vec();
        for b in &start {
            crlfstart.push(*b);
        }
        crlfstart
    };

    let inter = {
        let mut inter = CRLF.to_vec();
        for b in &bound {
            inter.push(*b);
        }
        inter
    };

    let mut s = State::Rest;
    let pos: std::cell::RefCell<usize> = std::cell::RefCell::new(0);
    let acc: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::new());

    let f = |m: &str| -> Result<Multipart> {
        bail!(
            "(pos {}/{} acc len {}) {}",
            pos.borrow(),
            data.len(),
            acc.borrow().len(),
            m
        );
    };

    let follows = |sample: &[u8]| -> bool {
        let pos = pos.borrow();

        if *pos + sample.len() < data.len() {
            sample == &data[*pos..*pos + sample.len()]
        } else {
            false
        }
    };

    loop {
        if *pos.borrow() >= data.len() {
            return f("unexpected end of multipart document");
        }

        match s {
            State::Rest => {
                *pos.borrow_mut() += if follows(&start) {
                    start.len()
                } else if follows(&crlfstart) {
                    crlfstart.len()
                } else {
                    return f("did not start with starting boundary");
                };
                s = State::Part;
                acc.borrow_mut().clear();
            }
            State::Part => {
                if follows(&inter) {
                    *pos.borrow_mut() += inter.len();
                    parts.push(acc.borrow().clone());
                    s = State::PartOrEnd;
                } else {
                    acc.borrow_mut().push(data[*pos.borrow()]);
                    *pos.borrow_mut() += 1;
                }
            }
            State::PartOrEnd => {
                let end = if follows(DASHDASH) {
                    *pos.borrow_mut() += DASHDASH.len();
                    true
                } else {
                    false
                };
                if follows(CRLF) {
                    *pos.borrow_mut() += CRLF.len();
                    if end {
                        break;
                    }
                    acc.borrow_mut().clear();
                    s = State::Part;
                    continue;
                } else if end {
                    break;
                } else {
                    println!(
                        "dbg: {:#?} end {} posdata {:?}",
                        mp,
                        end,
                        &data[*pos.borrow()..*pos.borrow() + 2]
                    );
                    return f("expected part or end");
                }
            }
        }
    }

    #[derive(Debug, PartialEq)]
    enum PartState {
        Header,
        HeaderCR,
        HeaderOrBody,
        HeaderOrBodyCR,
        Body,
    }

    for part in parts {
        let mut s = PartState::Header;
        let mut hdr = String::new();
        let mut hdrs: Vec<String> = Vec::new();
        let mut pos: usize = 0;

        let f = |m: &str| -> Result<Multipart> {
            bail!("{}", m);
        };

        let cr = b'\r';
        let lf = b'\n';

        let body: Vec<u8> = loop {
            if pos >= part.len() {
                return f("unexpected end of part");
            }
            let b = part[pos];

            match s {
                PartState::Header => {
                    if b > 127 {
                        return f("not 7-bit clean in header");
                    }
                    if b == cr {
                        hdrs.push(hdr.clone());
                        hdr.clear();
                        s = PartState::HeaderCR;
                        pos += 1;
                        continue;
                    }
                    if b == lf || b == 0 {
                        return f("malformed header");
                    }
                    hdr.push(b as char);
                    pos += 1;
                    continue;
                }
                PartState::HeaderCR => {
                    if b > 127 {
                        return f("not 7-bit clean in header");
                    }
                    if b == lf {
                        s = PartState::HeaderOrBody;
                        pos += 1;
                        continue;
                    }
                    return f("malformed header");
                }
                PartState::HeaderOrBody => {
                    if b > 127 {
                        return f("not 7-bit clean in header");
                    }
                    if b == cr {
                        s = PartState::HeaderOrBodyCR;
                        pos += 1;
                        continue;
                    }
                    s = PartState::Header;
                    continue;
                }
                PartState::HeaderOrBodyCR => {
                    if b > 127 {
                        return f("not 7-bit clean in header");
                    }
                    if b == lf {
                        s = PartState::Body;
                        pos += 1;
                        continue;
                    }
                    return f("malformed header");
                }
                PartState::Body => {
                    break part[pos..].to_vec();
                }
            }
        };

        let mut headers: HashMap<String, String> = HashMap::new();

        for hdr in &hdrs {
            let t: Vec<&str> = hdr.splitn(2, ':').collect();
            headers.insert(t[0].to_ascii_lowercase(), t[1].trim().to_string());
        }

        mp.parts.push(Part { headers, body });
    }

    Ok(mp)
}
