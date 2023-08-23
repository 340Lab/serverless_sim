use figlet_rs::FIGfont;

pub fn print_logo() {
    let standard_font = FIGfont::standard().unwrap();
    let figure = standard_font.convert("Serverless");
    assert!(figure.is_some());
    print!("{}", figure.unwrap());
    let figure = standard_font.convert("Sim");
    assert!(figure.is_some());
    println!("{}", figure.unwrap());
}
