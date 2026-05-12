package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"
)

// ══════════════════════════════════════════════════════════════════
// BLOOM FILTER
// ══════════════════════════════════════════════════════════════════

type BloomFilter struct {
	bits []uint64
	size uint64
	k    uint64
}

func newBloom(n int, fp float64) *BloomFilter {
	size := uint64(-float64(n) * math.Log(fp) / (math.Log(2) * math.Log(2)))
	k := uint64(math.Ceil(float64(size) / float64(n) * math.Log(2)))
	if k < 2 { k = 2 }
	if k > 20 { k = 20 }
	return &BloomFilter{bits: make([]uint64, (size+63)/64), size: size, k: k}
}

func (b *BloomFilter) hash(data []byte, seed uint64) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	for i := uint64(0); i < 8; i++ { buf[i] = byte(seed >> (i * 8)) }
	h.Write(buf[:])
	h.Write(data)
	return h.Sum64() % b.size
}

func (b *BloomFilter) TestAndSet(data []byte) bool {
	positions := make([]uint64, b.k)
	for i := uint64(0); i < b.k; i++ { positions[i] = b.hash(data, i*2654435761) }
	for _, pos := range positions {
		if b.bits[pos/64]>>(pos%64)&1 == 0 { goto set }
	}
	return true
set:
	for _, pos := range positions { b.bits[pos/64] |= 1 << (pos % 64) }
	return false
}

// ══════════════════════════════════════════════════════════════════
// BATCH CHANNEL
// ══════════════════════════════════════════════════════════════════

const batchSize = 2000

type Batch struct {
	items [][]byte
}

// sync.Pool pro recyklaci Batch objektů – žádné nové alokace
var batchPool = sync.Pool{
	New: func() any {
		b := &Batch{items: make([][]byte, 0, batchSize)}
		return b
	},
}

// sync.Pool pro strings.Builder – recyklace builderů v hot loops
var builderPool = sync.Pool{
	New: func() any { return &strings.Builder{} },
}

// ══════════════════════════════════════════════════════════════════
// WORD CACHE 
// ══════════════════════════════════════════════════════════════════

type WordCache struct {
	original  string
	lower     []byte
	upper     []byte
	cap       []byte
	leet      []byte
	leetCap   []byte
	allVars   [][]byte
}

func newWordCache(word string) *WordCache {
	lower   := []byte(strings.ToLower(word))
	upper   := []byte(strings.ToUpper(word))
	capStr  := []byte(capitalizeStr(word))
	leet    := []byte(toLeetStr(strings.ToLower(word)))
	leetCap := []byte(toLeetStr(capitalizeStr(word)))

	seen := map[string]struct{}{}
	var all [][]byte
	for _, v := range [][]byte{lower, upper, capStr, leet, leetCap} {
		k := string(v)
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			cp := make([]byte, len(v))
			copy(cp, v)
			all = append(all, cp)
		}
	}
	return &WordCache{
		original: word,
		lower:    lower, upper: upper, cap: capStr,
		leet:     leet, leetCap: leetCap, allVars: all,
	}
}

// ══════════════════════════════════════════════════════════════════
// LANGUAGE SYSTEM
// ══════════════════════════════════════════════════════════════════

type LanguageData struct {
	Months        []string            `json:"months"`
	Days          []string            `json:"days"`
	CommonSuffix  []string            `json:"common_suffixes"`
	KeyboardWalks []string            `json:"keyboard_walks"`
	Phonetic      map[string]string   `json:"phonetic"`
	Diminutives   map[string][]string `json:"diminutives"`
	Layout        string              `json:"layout"`
	DiacriticMap  map[string]string   `json:"diacritic_map"`
}

func loadLanguages(langDir string, langs []string) ([]LanguageData, error) {
	var result []LanguageData
	for _, lang := range langs {
		path := filepath.Join(langDir, lang+".json")
		f, err := os.Open(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[!] Jazyk '%s' nenalezen: %v\n", lang, err)
			continue
		}
		var ld LanguageData
		if err := json.NewDecoder(f).Decode(&ld); err != nil {
			f.Close()
			return nil, fmt.Errorf("chyba parsování %s: %w", path, err)
		}
		f.Close()
		result = append(result, ld)
		fmt.Printf("[*] Jazyk '%s' načten (%d měsíců, %d keyboard walks)\n",
			lang, len(ld.Months), len(ld.KeyboardWalks))
	}
	if len(result) == 0 {
		result = append(result, LanguageData{
			Months: []string{"01","02","03","04","05","06","07","08","09","10","11","12"},
			Days: []string{"monday","tuesday","wednesday","thursday","friday","saturday","sunday"},
			CommonSuffix: []string{"123","!","2024","1234"},
			KeyboardWalks: []string{"qwerty","asdf","zxcv"},
			Phonetic: map[string]string{}, Diminutives: map[string][]string{}, DiacriticMap: map[string]string{},
		})
	}
	return result, nil
}

func mergeLanguages(langs []LanguageData) LanguageData {
	merged := LanguageData{
		Phonetic: make(map[string]string),
		Diminutives: make(map[string][]string),
		DiacriticMap: make(map[string]string),
	}
	seen := map[string]struct{}{}
	add := func(s *[]string, v string) {
		if _, ok := seen[v]; !ok { seen[v] = struct{}{}; *s = append(*s, v) }
	}
	for _, l := range langs {
		for _, v := range l.Months        { add(&merged.Months, v) }
		for _, v := range l.Days          { add(&merged.Days, v) }
		for _, v := range l.CommonSuffix  { add(&merged.CommonSuffix, v) }
		for _, v := range l.KeyboardWalks { add(&merged.KeyboardWalks, v) }
		for k, v := range l.Phonetic      { merged.Phonetic[k] = v }
		for k, v := range l.Diminutives   { merged.Diminutives[k] = v }
		for k, v := range l.DiacriticMap  { merged.DiacriticMap[k] = v }
	}
	return merged
}

// ══════════════════════════════════════════════════════════════════
// DIACRITIC NORMALIZATION
// ══════════════════════════════════════════════════════════════════

var decomposeTable = map[rune]string{
	'à': "a", 'á': "a", 'â': "a", 'ã': "a", 'ä': "ae", 'å': "a", 'æ': "ae",
	'ç': "c", 'č': "c", 'ć': "c",
	'ď': "d", 'đ': "d",
	'è': "e", 'é': "e", 'ê': "e", 'ë': "e", 'ě': "e",
	'ì': "i", 'í': "i", 'î': "i", 'ï': "i",
	'ł': "l", 'ľ': "l",
	'ñ': "n", 'ň': "n", 'ń': "n",
	'ò': "o", 'ó': "o", 'ô': "o", 'õ': "o", 'ö': "oe", 'ø': "o",
	'ř': "r", 'ŕ': "r",
	'š': "s", 'ś': "s", 'ş': "s",
	'ť': "t", 'ţ': "t",
	'ù': "u", 'ú': "u", 'û': "u", 'ü': "ue", 'ů': "u",
	'ý': "y", 'ÿ': "y",
	'ž': "z", 'ź': "z", 'ż': "z",
	'ß': "ss",
	'À': "A", 'Á': "A", 'Â': "A", 'Ã': "A", 'Ä': "Ae", 'Å': "A", 'Æ': "Ae",
	'Ç': "C", 'Č': "C", 'Ć': "C",
	'Ď': "D",
	'È': "E", 'É': "E", 'Ê': "E", 'Ë': "E", 'Ě': "E",
	'Ì': "I", 'Í': "I", 'Î': "I", 'Ï': "I",
	'Ł': "L", 'Ľ': "L",
	'Ñ': "N", 'Ň': "N", 'Ń': "N",
	'Ò': "O", 'Ó': "O", 'Ô': "O", 'Õ': "O", 'Ö': "Oe", 'Ø': "O",
	'Ř': "R", 'Ŕ': "R",
	'Š': "S", 'Ś': "S",
	'Ť': "T",
	'Ù': "U", 'Ú': "U", 'Û': "U", 'Ü': "Ue", 'Ů': "U",
	'Ý': "Y",
	'Ž': "Z", 'Ź': "Z", 'Ż': "Z",
}

func removeDiacritics(s string, diacriticMap map[string]string) string {
	result := s
	for from, to := range diacriticMap {
		result = strings.ReplaceAll(result, from, to)
	}
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	for _, r := range result {
		if r <= 127 {
			b.WriteRune(r)
		} else if ascii, ok := decomposeTable[r]; ok {
			b.WriteString(ascii)
		} else {
			b.WriteRune(r)
		}
	}
	out := b.String()
	builderPool.Put(b)
	return out
}

// ══════════════════════════════════════════════════════════════════
// RULE ENGINE - Hashcat-style rule support
// ══════════════════════════════════════════════════════════════════

type Rule struct{ ops []string }

func loadRules(path string) ([]Rule, error) {
	f, err := os.Open(path)
	if err != nil { return nil, err }
	defer f.Close()
	var rules []Rule
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") { continue }
		rules = append(rules, Rule{ops: strings.Fields(line)})
	}
	return rules, sc.Err()
}

func applyRule(word string, rule Rule) string {
	s := word
	for _, op := range rule.ops {
		switch {
		case op == "l": s = strings.ToLower(s)
		case op == "u": s = strings.ToUpper(s)
		case op == "c": s = capitalizeStr(s)
		case op == "r": s = reverseStr(s)
		case op == "d": s = s + s
		case op == "t": s = toggleCase(s)
		case op == "i":
		case op == "[" && len(s) > 0: s = s[1:]
		case op == "]" && len(s) > 0: s = s[:len(s)-1]
		case strings.HasPrefix(op, "$"): s = s + op[1:]
		case strings.HasPrefix(op, "^"): s = op[1:] + s
		}
	}
	return s
}

// ══════════════════════════════════════════════════════════════════
// MASK ATTACK – stream iterator
// ══════════════════════════════════════════════════════════════════

func parseMaskSegments(mask string) [][]rune {
	digits := []rune("0123456789")
	lowers := []rune("abcdefghijklmnopqrstuvwxyz")
	uppers := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	all    := append(append(append([]rune{}, digits...), lowers...), append(uppers, []rune("!@#$%^&*")...)...)
	var segments [][]rune
	runes := []rune(mask)
	for i := 0; i < len(runes); i++ {
		if runes[i] == '?' && i+1 < len(runes) {
			switch runes[i+1] {
			case 'd': segments = append(segments, digits)
			case 'l': segments = append(segments, lowers)
			case 'u': segments = append(segments, uppers)
			case 'a': segments = append(segments, all)
			default:  segments = append(segments, []rune{runes[i+1]})
			}
			i++
		} else {
			segments = append(segments, []rune{runes[i]})
		}
	}
	return segments
}

func maskIterator(mask string, out chan<- string, stop <-chan struct{}) {
	segs := parseMaskSegments(mask)
	if len(segs) == 0 { return }
	indices := make([]int, len(segs))
	buf := make([]rune, len(segs))
	for {
		for i, idx := range indices { buf[i] = segs[i][idx] }
		select {
		case <-stop: return
		case out <- string(buf):
		}
		pos := len(segs) - 1
		for pos >= 0 {
			indices[pos]++
			if indices[pos] < len(segs[pos]) { break }
			indices[pos] = 0
			pos--
		}
		if pos < 0 { return }
	}
}

// ══════════════════════════════════════════════════════════════════
// LEETSPEAK
// ══════════════════════════════════════════════════════════════════

var leetMap = map[rune]string{
	'a': "4", 'e': "3", 'i': "1", 'o': "0",
	's': "5", 't': "7", 'l': "1", 'g': "9", 'b': "8",
}

// ══════════════════════════════════════════════════════════════════
// CORE EMITTER – zero-allocation batch pipeline
// ══════════════════════════════════════════════════════════════════

type Emitter struct {
	ch      chan<- *Batch
	current *Batch
	stop    <-chan struct{}
	minLen  int
	maxLen  int
}

func newEmitter(ch chan<- *Batch, stop <-chan struct{}, minLen, maxLen int) *Emitter {
	b := batchPool.Get().(*Batch)
	b.items = b.items[:0]
	return &Emitter{ch: ch, current: b, stop: stop, minLen: minLen, maxLen: maxLen}
}


func (e *Emitter) emit(s []byte) {
	l := len(s)
	if l < e.minLen || l > e.maxLen { return }
	cp := make([]byte, l)
	copy(cp, s)
	e.current.items = append(e.current.items, cp)
	if len(e.current.items) >= batchSize {
		e.flush()
	}
}


func (e *Emitter) emitStr(s string) {
	l := len(s)
	if l < e.minLen || l > e.maxLen { return }
	e.current.items = append(e.current.items, []byte(s))
	if len(e.current.items) >= batchSize {
		e.flush()
	}
}

func (e *Emitter) flush() {
	if len(e.current.items) == 0 { return }
	select {
	case <-e.stop: return
	case e.ch <- e.current:
	}
	b := batchPool.Get().(*Batch)
	b.items = b.items[:0]
	e.current = b
}

func (e *Emitter) done() { e.flush() }


func (e *Emitter) join(parts ...string) {
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	for _, p := range parts { b.WriteString(p) }
	e.emitStr(b.String())
	builderPool.Put(b)
}

// ══════════════════════════════════════════════════════════════════
// SMART PATTERNS
// ══════════════════════════════════════════════════════════════════

var separators = []string{"", "_", "-", ".", "!", "&", "+"}

func genSmartPatterns(caches []*WordCache, years []string, lang LanguageData, em *Emitter) {
	for _, wc := range caches {
		for _, v := range wc.allVars {
			sv := string(v)
			for _, y := range years {
				for _, sep := range separators {
					em.join(sv, sep, y)
				}
				em.join(capitalizeStr(wc.original), y, "!")
			}
		}
	}
	for i := range caches {
		for j := range caches {
			if i == j { continue }
			for _, sep := range []string{"", "_", "-", "&"} {
				combo := capitalizeStr(caches[i].original) + sep + capitalizeStr(caches[j].original)
				for _, y := range years {
					em.join(combo, y)
					em.join(combo, "_", y)
				}
				em.emitStr(combo)
				em.emitStr(strings.ToLower(combo))
			}
		}
	}
	pscPrefixes := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}
	for _, wc := range caches {
		for _, v := range wc.allVars {
			sv := string(v)
			for _, p := range pscPrefixes {
				em.join(sv, p, "0000")
				em.join(sv, "_", p, "0000")
			}
		}
	}
	for from, to := range lang.Phonetic {
		for _, wc := range caches {
			mutated := strings.ReplaceAll(strings.ToLower(wc.original), from, to)
			if mutated != strings.ToLower(wc.original) {
				em.emitStr(mutated)
				em.join(mutated, "123")
				em.join(capitalizeStr(mutated), "!")
			}
		}
	}
	for _, wc := range caches {
		lower := strings.ToLower(wc.original)
		if dims, ok := lang.Diminutives[lower]; ok {
			for _, dim := range dims {
				em.emitStr(dim)
				em.emitStr(capitalizeStr(dim))
				em.join(dim, "123")
				em.join(dim, "!")
				for _, y := range years {
					em.join(dim, y)
					em.join(capitalizeStr(dim), y)
				}
			}
		}
	}
}

// ══════════════════════════════════════════════════════════════════
// GENERÁTORY (priority skupiny) – všechny používají Emitter
// ══════════════════════════════════════════════════════════════════

func genPriority1(caches []*WordCache, years []string, lang LanguageData, em *Emitter) {
	top := dedupe(append([]string{"123","1234","12345","123456","1","!","!!","2024","2023","1!","123!","!123"}, lang.CommonSuffix...))
	for _, wc := range caches {
		for _, v := range wc.allVars {
			sv := string(v)
			em.emitStr(sv)
			for _, suf := range top { em.join(sv, suf) }
			for _, y := range years {
				em.join(sv, y)
				em.join(sv, "_", y)
				em.join(sv, "-", y)
			}
		}
	}
}

func genPriority2(caches []*WordCache, lang LanguageData, em *Emitter) {
	for _, wc := range caches {
		for _, v := range wc.allVars {
			sv := string(v)
			for _, walk := range lang.KeyboardWalks {
				em.join(sv, walk)
				em.join(walk, sv)
				em.join(sv, "_", walk)
			}
		}
	}
	for _, walk := range lang.KeyboardWalks {
		em.emitStr(walk)
		em.join(walk, "123")
		em.join(walk, "!")
	}
}

func genPriority3(caches []*WordCache, years []string, em *Emitter) {
	suf := []string{"1","12","123","1234","12345","123456","!","!!","!1","00","69","88","#","@","xd","lol"}
	seps := []string{"","_","-",".","!"}
	for i := range caches {
		for j := range caches {
			if i == j { continue }
			wi := strings.ToLower(caches[i].original)
			wj := strings.ToLower(caches[j].original)
			for _, sep := range seps {
				combo := wi + sep + wj
				for _, v := range generateVariantsStr(combo) {
					em.emitStr(v)
					for _, s := range suf { em.join(v, s) }
					for _, y := range years { em.join(v, y); em.join(v, "_", y) }
				}
			}
		}
	}
}

func genPriority4(caches []*WordCache, years []string, lang LanguageData, em *Emitter) {
	numMonths := []string{"01","02","03","04","05","06","07","08","09","10","11","12"}
	days := []string{"01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"}
	seps := []string{"","_","-",".","!"}
	for _, wc := range caches {
		for _, v := range wc.allVars {
			sv := string(v)
			for _, m := range numMonths { for _, sep := range seps { em.join(sv, sep, m) } }
			for _, d := range days      { for _, sep := range seps { em.join(sv, sep, d) } }
			for _, m := range numMonths {
				for _, d := range days {
					for _, sep := range seps {
						em.join(sv, sep, d, m)
						em.join(sv, sep, m, d)
					}
				}
			}
			for _, cm := range lang.Months { em.join(sv, cm); em.join(sv, "_", cm); em.join(cm, sv) }
			for _, cd := range lang.Days   { em.join(sv, cd); em.join(cd, sv) }
			for _, sep := range seps {
				for _, digit := range []string{"1","2","3","4","5","6","7","8","9","0"} {
					for r := 1; r <= 4; r++ { em.join(sv, sep, strings.Repeat(digit, r)) }
				}
			}
		}
	}
}

func genPriority5(caches []*WordCache, years []string, em *Emitter) {
	if len(caches) >= 3 {
		seps := []string{"","_","-",".","!"}
		for i := range caches {
			for j := range caches {
				if j == i { continue }
				for k := range caches {
					if k == i || k == j { continue }
						for _, sep := range seps {
						wi := strings.ToLower(caches[i].original)
						wj := strings.ToLower(caches[j].original)
						wk := strings.ToLower(caches[k].original)
						combo := wi + sep + wj + sep + wk
						em.emitStr(combo)
						em.emitStr(strings.ToUpper(combo))
						em.join(combo, "123")
						em.join(combo, "!")
						for _, y := range years { em.join(combo, y) }
					}
				}
			}
		}
	}
	for _, wc := range caches {
		for _, partial := range partialLeet(string(wc.lower)) {
			em.emitStr(partial)
			em.join(partial, "123")
			em.join(partial, "!")
		}
	}
}

// ══════════════════════════════════════════════════════════════════
// MAIN
// ══════════════════════════════════════════════════════════════════

func main() {
	fAge     := flag.Int("age", 0, "Věk cíle")
	fColor   := flag.String("color", "", "Oblíbená barva")
	fWord    := flag.String("word", "", "Oblíbené slovo")
	fPet     := flag.String("pet", "", "Jméno mazlíčka")
	fNum     := flag.String("num", "", "Čísla (čárkou)")
	fCustom  := flag.String("custom", "", "Custom slova (čárkou)")
	fSibling := flag.String("siblings", "", "Sourozenci (čárkou)")
	fGrand   := flag.String("grand", "", "Vnoučata (čárkou)")
	fYears   := flag.String("years", "", "Důležité roky (čárkou)")
	fOut     := flag.String("o", "", "Výstupní soubor (- = stdout)")
	fMin     := flag.Int("min", 6, "Minimální délka")
	fMax     := flag.Int("max", 32, "Maximální délka")
	fWorkers := flag.Int("workers", runtime.NumCPU(), "Počet goroutines")
	fRules   := flag.String("rules", "rules.rule", "Rule soubor")
	fMask    := flag.String("mask", "", "Maska (např. ?d?d)")
	fBloom   := flag.Bool("bloom", true, "Bloom Filter")
	fLang    := flag.String("lang", "cs", "Jazyky (čárkou): cs,en,de,ru,es")
	fLangDir := flag.String("langdir", "languages", "Složka s JSON jazyky")
	flag.Parse()

	interactive := flag.NFlag() == 0
	reader := bufio.NewReader(os.Stdin)

	var ageStr, colorStr, wordStr, petStr, numStr string
	var customStr, siblingStr, grandStr, yearsStr, outFile string
	var minLen, maxLen, workers int
	var rulesPath, maskStr, langStr, langDir string
	useBloom := true

	if interactive {
		fmt.Println("=== WORDLIST GENERATOR v3.0 ===")
		fmt.Println()
		fmt.Println("Vyber jazyk / Choose language / Sprache wählen / Выбери язык / Elige idioma:")
		fmt.Println("  cs  –  Čeština")
		fmt.Println("  en  –  English")
		fmt.Println("  de  –  Deutsch")
		fmt.Println("  ru  –  Русский")
		fmt.Println("  es  –  Español")
		fmt.Println()
		fmt.Print("> ")
		primaryLang, _ := readLine(reader)
		if primaryLang == "" { primaryLang = "cs" }

		examples := map[string]string{"cs":"en","en":"cs","de":"cs","ru":"en","es":"en"}
		example := examples[primaryLang]
		if example == "" { example = "en" }
		var addLangPrompt string
		switch primaryLang {
		case "en": addLangPrompt = "Add another language? (Enter = no, or e.g. " + example + "): "
		case "de": addLangPrompt = "Weitere Sprache? (Enter = nein, z.B. " + example + "): "
		case "ru": addLangPrompt = "Добавить язык? (Enter = нет, напр. " + example + "): "
		case "es": addLangPrompt = "¿Otro idioma? (Enter = no, p.ej. " + example + "): "
		default:   addLangPrompt = "Přidat další jazyk? (Enter = ne, např. " + example + "): "
		}
		fmt.Print(addLangPrompt)
		extraLang, _ := readLine(reader)
		if extraLang != "" { langStr = primaryLang + "," + extraLang } else { langStr = primaryLang }
		fmt.Println()

		switch primaryLang {
		case "en": fmt.Println("Press Enter or type '-' to skip a field.")
		case "de": fmt.Println("Enter oder '-' zum Überspringen.")
		case "ru": fmt.Println("Enter или '-' для пропуска поля.")
		case "es": fmt.Println("Enter o '-' para saltar un campo.")
		default:   fmt.Println("Enter nebo '-' pro přeskočení.")
		}
		fmt.Println()

		ageStr     = readInput(reader, "Věk cíle: ")
		colorStr   = readInput(reader, "Oblíbená barva: ")
		wordStr    = readInput(reader, "Oblíbené slovo: ")
		petStr     = readInput(reader, "Jméno mazlíčka: ")
		numStr     = readInput(reader, "Oblíbená čísla (čárkou): ")
		customStr  = readInput(reader, "Custom slova (čárkou): ")
		siblingStr = readInput(reader, "Sourozenci (čárkou): ")
		grandStr   = readInput(reader, "Vnoučata (čárkou): ")
		yearsStr   = readInput(reader, "Důležité roky (čárkou): ")
		fmt.Print("Název souboru (Enter = auto, - = stdout): ")
		outFile, _ = readLine(reader)
		fmt.Print("Min délka (Enter = 6): ")
		s1, _ := readLine(reader)
		fmt.Print("Max délka (Enter = 32): ")
		s2, _ := readLine(reader)
		fmt.Print("Rule soubor (Enter = rules.rule): ")
		rulesPath, _ = readLine(reader)
		fmt.Print("Maska (Enter = přeskočit, např. ?d?d): ")
		maskStr, _ = readLine(reader)
		minLen = parseIntDefault(s1, 6)
		maxLen = parseIntDefault(s2, 32)
		workers = runtime.NumCPU()
		langDir = "languages"
		if rulesPath == "" { rulesPath = "rules.rule" }
	} else {
		if *fAge > 0 { ageStr = strconv.Itoa(*fAge) }
		colorStr = *fColor; wordStr = *fWord; petStr = *fPet
		numStr = *fNum; customStr = *fCustom; siblingStr = *fSibling
		grandStr = *fGrand; yearsStr = *fYears; outFile = *fOut
		minLen = *fMin; maxLen = *fMax; workers = *fWorkers
		rulesPath = *fRules; maskStr = *fMask; useBloom = *fBloom
		langStr = *fLang; langDir = *fLangDir
	}

	// Langue
	langs := splitInput(langStr)
	if len(langs) == 0 { langs = []string{"cs"} }
	langDataList, err := loadLanguages(langDir, langs)
	if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
	lang := mergeLanguages(langDataList)

	
	var rawWords []string
	for _, w := range []string{colorStr, wordStr, petStr} {
		if w != "" { rawWords = append(rawWords, w) }
	}
	for _, s := range splitInput(numStr)     { rawWords = append(rawWords, s) }
	for _, s := range splitInput(customStr)  { rawWords = append(rawWords, s) }
	for _, s := range splitInput(siblingStr) { rawWords = append(rawWords, s) }
	for _, s := range splitInput(grandStr)   { rawWords = append(rawWords, s) }
	if len(rawWords) == 0 { fmt.Fprintln(os.Stderr, "Chyba: zadej alespoň jedno slovo."); os.Exit(1) }

	var allWords []string
	for _, w := range rawWords {
		allWords = append(allWords, w)
		stripped := removeDiacritics(w, lang.DiacriticMap)
		if stripped != w { allWords = append(allWords, stripped) }
	}
	allWords = dedupe(allWords)
	if len(allWords) > len(rawWords) {
		fmt.Printf("[*] Diakritika: přidáno %d normalizovaných variant\n", len(allWords)-len(rawWords))
	}

	
	caches := make([]*WordCache, len(allWords))
	for i, w := range allWords { caches[i] = newWordCache(w) }

	
	currentYear := time.Now().Year()
	minYear := 1950
	if ageStr != "" {
		vek, err := strconv.Atoi(ageStr)
		if err == nil && vek > 0 {
			by := currentYear - vek - 1
			minYear = by - 1
			fmt.Printf("[*] Rok narození %d nebo %d, roky %d–%d\n", by, by+1, minYear, currentYear)
		}
	}
	var years []string
	for y := minYear; y <= currentYear; y++ {
		years = append(years, fmt.Sprintf("%d", y))
		years = append(years, fmt.Sprintf("%02d", y%100))
	}
	for _, s := range splitInput(yearsStr) { years = append(years, s) }
	years = dedupe(years)

	// Rules
	var rules []Rule
	if rulesPath != "" {
		r, err := loadRules(rulesPath)
		if err == nil && len(r) > 0 {
			rules = r
			fmt.Printf("[*] Načteno %d pravidel z %s\n", len(rules), rulesPath)
		}
	}

	if maskStr != "" {
		segs := parseMaskSegments(maskStr)
		total := 1
		for _, s := range segs { total *= len(s) }
		fmt.Printf("[*] Maska '%s' → %d kombinací (stream)\n", maskStr, total)
	}


	var outWriter *bufio.Writer
	var outFile2 *os.File
	if outFile == "-" || outFile == "" && interactive == false {
		outWriter = bufio.NewWriterSize(os.Stdout, 4<<20)
	} else {
		if outFile == "" { outFile = "wordlist_" + time.Now().Format("20060102_1504") + ".txt" }
		if !strings.HasSuffix(outFile, ".txt") { outFile += ".txt" }
		outFile2, err = os.Create(outFile)
		if err != nil { fmt.Fprintln(os.Stderr, "Chyba:", err); os.Exit(1) }
		defer outFile2.Close()
		outWriter = bufio.NewWriterSize(outFile2, 4<<20)
	}
	defer outWriter.Flush()

	// Bloom filter
	var bloom *BloomFilter
	if useBloom {
		bloom = newBloom(5_000_000, 0.01)
		fmt.Println("[*] Bloom Filter aktivní (RAM ~7MB, FP 1%)")
	}

	// Graceful shutdown
	stopCh := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\n[!] Přerušeno – zapisuji zbývající data...")
		close(stopCh)
	}()

	// Batch channel
	batchCh := make(chan *Batch, workers*8)
	var written atomic.Int64
	var writerWg sync.WaitGroup
	writerWg.Add(1)

	// Writer goroutine
	go func() {
		defer writerWg.Done()
		for batch := range batchCh {
			for _, item := range batch.items {
				if bloom != nil && bloom.TestAndSet(item) { continue }
				outWriter.Write(item)
				outWriter.WriteByte('\n')
				n := written.Add(1)
				if n%500000 == 0 { outWriter.Flush(); printProgress(n) }
			}
			batch.items = batch.items[:0]
			batchPool.Put(batch)
		}
	}()

	fmt.Printf("[*] Generuji (%d jader)...\n", workers)
	start := time.Now()

	newEm := func() *Emitter { return newEmitter(batchCh, stopCh, minLen, maxLen) }

	runSeq := func(fn func(*Emitter)) {
		em := newEm()
		fn(em)
		em.done()
	}

	runParallel := func(fns []func(*Emitter)) {
		var wg sync.WaitGroup
		sem := make(chan struct{}, workers)
		for _, fn := range fns {
			fn := fn
			wg.Add(1); sem <- struct{}{}
			go func() {
				defer wg.Done(); defer func() { <-sem }()
				em := newEm()
				fn(em)
				em.done()
			}()
		}
		wg.Wait()
	}

	// ── Priority 1
	runSeq(func(em *Emitter) { genPriority1(caches, years, lang, em) })
	// ── Priority 2 – keyboard walks
	runSeq(func(em *Emitter) { genPriority2(caches, lang, em) })
	// ── Priority 3 – dvě slova (paralelně)
	var p3 []func(*Emitter)
	for i := range caches {
		i := i
		p3 = append(p3, func(em *Emitter) {
			sub := append([]*WordCache{caches[i]}, caches...)
			genPriority3(sub, years, em)
		})
	}
	runParallel(p3)
	// ── Priority 4 
	var p4 []func(*Emitter)
	for i := range caches {
		i := i
		p4 = append(p4, func(em *Emitter) { genPriority4([]*WordCache{caches[i]}, years, lang, em) })
	}
	runParallel(p4)
	// ── Priority 5 
	runSeq(func(em *Emitter) { genPriority5(caches, years, em) })
	// ── Smart Patterns
	runSeq(func(em *Emitter) { genSmartPatterns(caches, years, lang, em) })
	// ── Rule Engine
	if len(rules) > 0 {
		var rf []func(*Emitter)
		for _, rule := range rules {
			rule := rule
			rf = append(rf, func(em *Emitter) {
				for _, wc := range caches {
					for _, v := range wc.allVars {
						em.emitStr(applyRule(string(v), rule))
					}
				}
			})
		}
		runParallel(rf)
	}
	// ── Mask Attack
	if maskStr != "" {
		maskCh := make(chan string, workers*1000)
		go func() { defer close(maskCh); maskIterator(maskStr, maskCh, stopCh) }()
		em := newEm()
		for ms := range maskCh {
			for _, wc := range caches {
				for _, v := range wc.allVars {
					em.join(string(v), ms)
					em.join(ms, string(v))
				}
			}
		}
		em.done()
	}

	select { case <-stopCh: default: }
	close(batchCh)
	writerWg.Wait()
	outWriter.Flush()

	elapsed := time.Since(start).Round(time.Millisecond)
	n := written.Load()
	if outFile != "" && outFile != "-" {
		fmt.Fprintf(os.Stderr, "\r[✓] %d hesel → %s (%s)\n", n, outFile, elapsed)
		fmt.Fprintf(os.Stderr, "    Finální dedup: sort -u %s -o %s\n", outFile, outFile)
	} else {
		fmt.Fprintf(os.Stderr, "\r[✓] %d hesel vygenerováno (%s)\n", n, elapsed)
	}
}

// ══════════════════════════════════════════════════════════════════
// HELPERS
// ══════════════════════════════════════════════════════════════════

func readInput(r *bufio.Reader, prompt string) string {
	fmt.Print(prompt)
	line, _ := r.ReadString('\n')
	line = strings.TrimSpace(line)
	if line == "" || line == "-" { return "" }
	return line
}
func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	return strings.TrimSpace(line), err
}
func parseIntDefault(s string, def int) int {
	if s == "" { return def }
	v, err := strconv.Atoi(s)
	if err != nil { return def }
	return v
}
func splitInput(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" && p != "-" { out = append(out, p) }
	}
	return out
}
func generateVariantsStr(word string) []string {
	lower := strings.ToLower(word)
	upper := strings.ToUpper(word)
	cap   := capitalizeStr(word)
	leet  := toLeetStr(lower)
	leetC := toLeetStr(cap)
	return dedupe([]string{lower, upper, cap, leet, leetC})
}
func partialLeet(s string) []string {
	runes := []rune(s)
	var pos []int
	for i, r := range runes {
		if _, ok := leetMap[r]; ok { pos = append(pos, i) }
	}
	if len(pos) == 0 { return []string{s} }
	max := 1 << len(pos)
	if max > 16 { max = 16 }
	var results []string
	for mask := 0; mask < max; mask++ {
		tmp := make([]rune, len(runes))
		copy(tmp, runes)
		for bit, p := range pos {
			if mask&(1<<bit) != 0 { tmp[p] = []rune(leetMap[runes[p]])[0] }
		}
		results = append(results, string(tmp))
	}
	return dedupe(results)
}
func capitalizeStr(s string) string {
	if s == "" { return s }
	r := []rune(s)
	r[0] = unicode.ToUpper(r[0])
	for i := 1; i < len(r); i++ { r[i] = unicode.ToLower(r[i]) }
	return string(r)
}
func toLeetStr(s string) string {
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	for _, r := range s {
		if rep, ok := leetMap[r]; ok { b.WriteString(rep) } else { b.WriteRune(r) }
	}
	out := b.String()
	builderPool.Put(b)
	return out
}
func reverseStr(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 { r[i], r[j] = r[j], r[i] }
	return string(r)
}
func toggleCase(s string) string {
	b := builderPool.Get().(*strings.Builder)
	b.Reset()
	for _, r := range s {
		if unicode.IsUpper(r) { b.WriteRune(unicode.ToLower(r)) } else { b.WriteRune(unicode.ToUpper(r)) }
	}
	out := b.String()
	builderPool.Put(b)
	return out
}
func dedupe(ss []string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, s := range ss {
		if _, ok := seen[s]; !ok { seen[s] = struct{}{}; out = append(out, s) }
	}
	return out
}
func printProgress(n int64) {
	bars := int(n/500000) % 40
	fmt.Fprintf(os.Stderr, "\r[%s%s] %dM hesel",
		strings.Repeat("█", bars), strings.Repeat("░", 40-bars), n/1_000_000)
}
