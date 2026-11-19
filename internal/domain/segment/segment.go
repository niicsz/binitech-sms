package segment

import (
	"errors"
	"strings"
	"unicode/utf8"
)

type Encoding string

const (
	GSM7 Encoding = "GSM7"
	UCS2 Encoding = "UCS2"
)

const (
	MaxSegments     = 10
	gsm7Single      = 160
	gsm7Multipart   = 153
	ucs2Single      = 70
	ucs2Multipart   = 67
	gsm7ExtWidth    = 2
	surrogateWidth  = 2
	basicCharacters = "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"
	extCharacters   = "^{}\\[~]|€\f"
)

var (
	ErrEmpty   = errors.New("segment: texto vazio")
	ErrTooLong = errors.New("segment: texto passa do limite de segmentos")
	ErrInvalid = errors.New("segment: texto com UTF-8 inválido")
)

var (
	basicSet = runeSet(basicCharacters)
	extSet   = runeSet(extCharacters)
)

var normalizer = strings.NewReplacer(
	"“", "\"", "”", "\"", "„", "\"",
	"‘", "'", "’", "'", "‚", "'",
	"–", "-", "—", "-",
	"…", "...",
	" ", " ",
)

type Result struct {
	Encoding Encoding
	Units    int
	Segments int
}

func Analyze(text string) (Result, error) {
	if text == "" {
		return Result{}, ErrEmpty
	}
	if !utf8.ValidString(text) {
		return Result{}, ErrInvalid
	}
	var result Result
	if isGSM7(text) {
		result = pack(text, GSM7, gsm7Single, gsm7Multipart, gsm7Width)
	} else {
		result = pack(text, UCS2, ucs2Single, ucs2Multipart, ucs2Width)
	}
	if result.Segments > MaxSegments {
		return result, ErrTooLong
	}
	return result, nil
}

func Normalize(text string) string {
	return normalizer.Replace(text)
}

func isGSM7(text string) bool {
	for _, r := range text {
		if !basicSet[r] && !extSet[r] {
			return false
		}
	}
	return true
}

func gsm7Width(r rune) int {
	if extSet[r] {
		return gsm7ExtWidth
	}
	return 1
}

func ucs2Width(r rune) int {
	if r > 0xFFFF {
		return surrogateWidth
	}
	return 1
}

func pack(text string, encoding Encoding, single, multipart int, width func(rune) int) Result {
	total := 0
	for _, r := range text {
		total += width(r)
	}
	if total <= single {
		return Result{Encoding: encoding, Units: total, Segments: 1}
	}
	segments, used := 1, 0
	for _, r := range text {
		w := width(r)
		if used+w > multipart {
			segments++
			used = 0
		}
		used += w
	}
	return Result{Encoding: encoding, Units: total, Segments: segments}
}

func runeSet(chars string) map[rune]bool {
	set := make(map[rune]bool, utf8.RuneCountInString(chars))
	for _, r := range chars {
		set[r] = true
	}
	return set
}
