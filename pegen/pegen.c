#include <Python.h>
#include "pegen.h"
#include "v38tokenizer.h"

static inline PyObject *
new_identifier(Parser *p, char *identifier)
{
    PyObject *id = PyUnicode_FromString(identifier);
    if (id == NULL) {
        return NULL;
    }
    if (PyArena_AddPyObject(p->arena, id) < 0) {
        Py_DECREF(id);
        return NULL;
    }
    return id;
}

static PyObject *
_create_dummy_identifier(Parser *p)
{
    return new_identifier(p, "");
}

static inline Py_ssize_t
byte_offset_to_character_offset(PyObject *line, int col_offset)
{
    const char *str = PyUnicode_AsUTF8(line);
    PyObject *text = PyUnicode_DecodeUTF8(str, col_offset, NULL);
    if (!text) {
        return 0;
    }
    Py_ssize_t size = PyUnicode_GET_LENGTH(text);
    Py_DECREF(text);
    return size;
}

int
raise_syntax_error(Parser *p, const char *errmsg, ...)
{
    PyObject *value = NULL;
    PyObject *errstr = NULL;
    PyObject *loc = NULL;
    PyObject *tmp = NULL;
    PyObject *filename = NULL;
    Token *t = p->tokens[p->fill - 1];
    va_list va;

    va_start(va, errmsg);
    errstr = PyUnicode_FromFormatV(errmsg, va);
    va_end(va);
    if (!errstr) {
        goto error;
    }
    if (p->tok->filename) {
        filename = p->tok->filename;
        loc = PyErr_ProgramTextObject(filename, t->lineno);
        if (!loc) {
            Py_INCREF(Py_None);
            loc = Py_None;
        }
    }
    else {
        Py_INCREF(Py_None);
        filename = Py_None;
        loc = PyUnicode_FromString(p->tok->buf);
        if (!loc) {
            goto error;
        }
    }
    Py_ssize_t col_number = byte_offset_to_character_offset(loc, t->col_offset) + 1;
    tmp = Py_BuildValue("(OiiN)", filename, t->lineno, col_number, loc);
    if (!tmp) {
        goto error;
    }
    value = PyTuple_Pack(2, errstr, tmp);
    Py_DECREF(tmp);
    if (!value) {
        goto error;
    }
    PyErr_SetObject(PyExc_SyntaxError, value);
    Py_DECREF(errstr);
    Py_DECREF(value);
    return 0;

error:
    Py_XDECREF(errstr);
    if (!p->tok->filename) {
        Py_XDECREF(filename);
    }
    Py_XDECREF(loc);
    return -1;
}

static const char *
token_name(int type)
{
    if (0 <= type && type <= N_TOKENS) {
        return _PyParser_TokenNames[type];
    }
    return "<Huh?>";
}

// Here, mark is the start of the node, while p->mark is the end.
// If node==NULL, they should be the same.
int
insert_memo(Parser *p, int mark, int type, void *node)
{
    // Insert in front
    Memo *m = PyArena_Malloc(p->arena, sizeof(Memo));
    if (m == NULL) {
        return -1;
    }
    m->type = type;
    m->node = node;
    m->mark = p->mark;
    m->next = p->tokens[mark]->memo;
    p->tokens[mark]->memo = m;
    return 0;
}

// Like insert_memo(), but updates an existing node if found.
int
update_memo(Parser *p, int mark, int type, void *node)
{
    for (Memo *m = p->tokens[mark]->memo; m != NULL; m = m->next) {
        if (m->type == type) {
            // Update existing node.
            m->node = node;
            m->mark = p->mark;
            return 0;
        }
    }
    // Insert new node.
    return insert_memo(p, mark, type, node);
}

// Return dummy NAME.
void *
CONSTRUCTOR(Parser *p, ...)
{
    static void *cache = NULL;

    if (cache != NULL) {
        return cache;
    }

    PyObject *id = _create_dummy_identifier(p);
    if (!id) {
        return NULL;
    }
    cache = Name(id, Load, 1, 0, 1, 0, p->arena);
    return cache;
}

static int
_get_keyword_or_name_type(Parser *p, char *name, int name_len)
{
    if (name_len >= p->n_keyword_lists || p->keywords[name_len] == NULL) {
        return NAME;
    }
    for (KeywordToken *k = p->keywords[name_len]; k->type != -1; k++) {
        if (strncmp(k->str, name, name_len) == 0) {
            return k->type;
        }
    }
    return NAME;
}

int
fill_token(Parser *p)
{
    char *start, *end;
    int type = PyTokenizer_Get(p->tok, &start, &end);
    if (type == ERRORTOKEN) {
        if (!PyErr_Occurred()) {
            PyErr_Format(PyExc_SyntaxError, "Tokenizer returned error token");
            // There is no reliable column information for this error
            PyErr_SyntaxLocationObject(p->tok->filename, p->tok->lineno, 0);
        }
        return -1;
    }

    if (p->fill == p->size) {
        int newsize = p->size * 2;
        p->tokens = PyMem_Realloc(p->tokens, newsize * sizeof(Token *));
        if (p->tokens == NULL) {
            PyErr_Format(PyExc_MemoryError, "Realloc tokens failed");
            return -1;
        }
        for (int i = p->size; i < newsize; i++) {
            p->tokens[i] = PyMem_Malloc(sizeof(Token));
            memset(p->tokens[i], '\0', sizeof(Token));
        }
        p->size = newsize;
    }

    Token *t = p->tokens[p->fill];
    t->type = (type == NAME) ? _get_keyword_or_name_type(p, start, (int) (end - start)) : type;
    t->bytes = PyBytes_FromStringAndSize(start, end - start);
    if (t->bytes == NULL) {
        return -1;
    }
    PyArena_AddPyObject(p->arena, t->bytes);

    int lineno = type == STRING ? p->tok->first_lineno : p->tok->lineno;
    const char *line_start = type == STRING ? p->tok->multi_line_start : p->tok->line_start;
    int end_lineno = p->tok->lineno;
    int col_offset = -1, end_col_offset = -1;
    if (start != NULL && start >= line_start) {
        col_offset = start - line_start;
    }
    if (end != NULL && end >= p->tok->line_start) {
        end_col_offset = end - p->tok->line_start;
    }

    t->lineno = lineno;
    t->col_offset = col_offset;
    t->end_lineno = end_lineno;
    t->end_col_offset = end_col_offset;

    // if (p->fill % 100 == 0) fprintf(stderr, "Filled at %d: %s \"%s\"\n", p->fill,
    // token_name(type), PyBytes_AsString(t->bytes));
    p->fill += 1;
    return 0;
}

int  // bool
is_memoized(Parser *p, int type, void *pres)
{
    if (p->mark == p->fill) {
        if (fill_token(p) < 0) {
            return -1;
        }
    }

    Token *t = p->tokens[p->mark];

    for (Memo *m = t->memo; m != NULL; m = m->next) {
        if (m->type == type) {
            p->mark = m->mark;
            *(void **)(pres) = m->node;
            // fprintf(stderr, "%d < %d: memoized!\n", p->mark, p->fill);
            return 1;
        }
    }
    // fprintf(stderr, "%d < %d: not memoized\n", p->mark, p->fill);
    return 0;
}

int
lookahead_with_string(int positive, void *(func)(Parser *, const char *), Parser *p,
                      const char *arg)
{
    int mark = p->mark;
    void *res = func(p, arg);
    p->mark = mark;
    return (res != NULL) == positive;
}

int
lookahead_with_int(int positive, Token *(func)(Parser *, int), Parser *p, int arg)
{
    int mark = p->mark;
    void *res = func(p, arg);
    p->mark = mark;
    return (res != NULL) == positive;
}

int
lookahead(int positive, void *(func)(Parser *), Parser *p)
{
    int mark = p->mark;
    void *res = func(p);
    p->mark = mark;
    return (res != NULL) == positive;
}

Token *
expect_token(Parser *p, int type)
{
    if (p->mark == p->fill) {
        if (fill_token(p) < 0) {
            return NULL;
        }
    }
    Token *t = p->tokens[p->mark];
    if (t->type != type) {
        // fprintf(stderr, "No %s at %d\n", token_name(type), p->mark);
        return NULL;
    }
    p->mark += 1;
    // fprintf(stderr, "Got %s at %d: %s\n", token_name(type), p->mark,
    // PyBytes_AsString(t->bytes));

    return t;
}

Token *
get_last_nonnwhitespace_token(Parser *p)
{
    assert(p->mark >= 0);
    Token *token = NULL;
    for (int m = p->mark - 1; m >= 0; m--) {
        token = p->tokens[m];
        if (token->type != ENDMARKER && (token->type < NEWLINE || token->type > DEDENT)) {
            break;
        }
    }
    return token;
}

void *
async_token(Parser *p)
{
    return expect_token(p, ASYNC);
}

void *
await_token(Parser *p)
{
    return expect_token(p, AWAIT);
}

void *
endmarker_token(Parser *p)
{
    return expect_token(p, ENDMARKER);
}

expr_ty
name_token(Parser *p)
{
    Token *t = expect_token(p, NAME);
    if (t == NULL) {
        return NULL;
    }
    char *s;
    Py_ssize_t n;
    if (PyBytes_AsStringAndSize(t->bytes, &s, &n) < 0) {
        return NULL;
    }
    PyObject *id = PyUnicode_DecodeUTF8(s, n, NULL);
    if (id == NULL) {
        return NULL;
    }
    if (PyArena_AddPyObject(p->arena, id) < 0) {
        Py_DECREF(id);
        return NULL;
    }
    // TODO: What new_identifier() does.
    return Name(id, Load, t->lineno, t->col_offset, t->end_lineno, t->end_col_offset,
                p->arena);
}

void *
newline_token(Parser *p)
{
    return expect_token(p, NEWLINE);
}

void *
indent_token(Parser *p)
{
    return expect_token(p, INDENT);
}

void *
dedent_token(Parser *p)
{
    return expect_token(p, DEDENT);
}

expr_ty
number_token(Parser *p)
{
    Token *t = expect_token(p, NUMBER);
    if (t == NULL) {
        return NULL;
    }
    // TODO: Just copy CPython's machinery for parsing numbers.
    PyObject *c = PyLong_FromString(PyBytes_AsString(t->bytes), (char **)0, 0);
    if (c == NULL) {
        PyErr_Clear();
        PyObject *tbytes = t->bytes;
        Py_ssize_t size = PyBytes_Size(tbytes);
        char *bytes = PyBytes_AsString(tbytes);
        char lastc = size == 0 ? 0 : bytes[size - 1];
        int iscomplex = 0;
        PyObject *obytes = NULL;
        if (size > 0 && (lastc == 'j' || lastc == 'J')) {
            iscomplex = 1;
            obytes = PyBytes_FromStringAndSize(bytes, size - 1);
            if (obytes == NULL) {
                return NULL;
            }
            tbytes = obytes;
        }
        c = PyFloat_FromString(tbytes);
        Py_XDECREF(obytes);
        if (c == NULL) {
            return NULL;
        }
        if (iscomplex) {
            double real = PyFloat_AsDouble(c);
            double imag = 0;
            Py_DECREF(c);
            c = PyComplex_FromDoubles(real, imag);
            if (c == NULL) {
                return NULL;
            }
        }
    }
    if (PyArena_AddPyObject(p->arena, c) < 0) {
        Py_DECREF(c);
        return NULL;
    }
    return Constant(c, NULL, t->lineno, t->col_offset, t->end_lineno, t->end_col_offset,
                    p->arena);
}

static int
parsestr(Parser *p, const char *s, int *bytesmode, int *rawmode, PyObject **result,
         const char **fstr, Py_ssize_t *fstrlen);

expr_ty string_token(Parser *p);

void *
keyword_token(Parser *p, const char *val)
{
    int mark = p->mark;
    Token *t = expect_token(p, NAME);
    if (t == NULL) {
        return NULL;
    }
    if (strcmp(val, PyBytes_AsString(t->bytes)) == 0) {
        return t;
    }
    p->mark = mark;
    return NULL;
}

PyObject *
run_parser(struct tok_state *tok,
           void *(start_rule_func)(Parser *),
           int mode,
           KeywordToken **keywords,
           int n_keyword_lists)
{
    PyObject *result = NULL;
    Parser *p = PyMem_Malloc(sizeof(Parser));
    if (p == NULL) {
        PyErr_Format(PyExc_MemoryError, "Out of memory for Parser");
        goto exit;
    }
    assert(tok != NULL);
    p->tok = tok;
    p->keywords = keywords;
    p->n_keyword_lists = n_keyword_lists;
    p->tokens = PyMem_Malloc(sizeof(Token *));
    if (!p->tokens) {
        PyErr_Format(PyExc_MemoryError, "Out of memory for tokens");
        goto exit;
    }
    p->tokens[0] = PyMem_Malloc(sizeof(Token));
    memset(p->tokens[0], '\0', sizeof(Token));
    p->mark = 0;
    p->fill = 0;
    p->size = 1;

    p->arena = PyArena_New();
    if (!p->arena) {
        goto exit;
    }

    if (fill_token(p) < 0) {
        goto exit;
    }

    PyErr_Clear();

    p->start_rule_func = start_rule_func;

    void *res = (*start_rule_func)(p);
    if (res == NULL) {
        if (PyErr_Occurred()) {
            goto exit;
        }
        if (p->fill == 0) {
            raise_syntax_error(p, "error at start before reading any input");
        }
        else {
            raise_syntax_error(p, "invalid syntax");
        }
        goto exit;
    }

    if (mode == 2) {
        PyObject *filename = tok->filename ? tok->filename : PyUnicode_FromString("<string>");
        if (!filename) {
            goto exit;
        }
        result = (PyObject *)PyAST_CompileObject(res, filename, NULL, -1, p->arena);
        if (!tok->filename) {
            Py_XDECREF(filename);
        }
    }
    else if (mode == 1) {
        result = PyAST_mod2obj(res);
    }
    else {
        result = Py_None;
        Py_INCREF(result);
    }

exit:

    for (int i = 0; i < p->size; i++) {
        PyMem_Free(p->tokens[i]);
    }
    PyMem_Free(p->tokens);
    if (p->arena != NULL) {
        PyArena_Free(p->arena);
    }
    PyMem_Free(p);
    return result;
}


PyObject *
run_parser_from_file(const char *filename,
                     void *(start_rule_func)(Parser *),
                     int mode,
                     KeywordToken **keywords,
                     int n_keyword_lists)
{
    FILE *fp = fopen(filename, "rb");
    if (fp == NULL) {
        PyErr_SetFromErrnoWithFilename(PyExc_OSError, filename);
        return NULL;
    }

    PyObject *filename_ob = NULL;
    if ((filename_ob = PyUnicode_FromString(filename)) == NULL) {
        return NULL;
    }

    // From here on we need to clean up even if there's an error
    PyObject *result = NULL;

    struct tok_state *tok = PyTokenizer_FromFile(fp, NULL, NULL, NULL);
    if (tok == NULL) {
        goto error;
    }

    // Transfers ownership
    tok->filename = filename_ob;
    filename_ob = NULL;

    result = run_parser(tok, start_rule_func, mode, keywords, n_keyword_lists);

    PyTokenizer_Free(tok);

error:
    fclose(fp);
    Py_XDECREF(filename_ob);
    return result;
}

PyObject *
run_parser_from_string(const char *str,
                       void *(start_rule_func)(Parser *),
                       int mode,
                       KeywordToken **keywords,
                       int n_keyword_lists)
{
    struct tok_state *tok = PyTokenizer_FromString(str, 1);

    if (tok == NULL) {
        return NULL;
    }

    PyObject *result = run_parser(tok, start_rule_func, mode, keywords, n_keyword_lists);
    PyTokenizer_Free(tok);
    return result;
}

/* Creates a single-element asdl_seq* that contains a */
asdl_seq *
singleton_seq(Parser *p, void *a)
{
    asdl_seq *seq = _Py_asdl_seq_new(1, p->arena);
    if (!seq) {
        return NULL;
    }
    asdl_seq_SET(seq, 0, a);
    return seq;
}

/* Creates a copy of seq and prepends a to it */
asdl_seq *
seq_insert_in_front(Parser *p, void *a, asdl_seq *seq)
{
    if (!seq) {
        return singleton_seq(p, a);
    }

    asdl_seq *new_seq = _Py_asdl_seq_new(asdl_seq_LEN(seq) + 1, p->arena);
    if (!new_seq) {
        return NULL;
    }

    asdl_seq_SET(new_seq, 0, a);
    for (int i = 1, l = asdl_seq_LEN(new_seq); i < l; i++) {
        asdl_seq_SET(new_seq, i, asdl_seq_GET(seq, i - 1));
    }
    return new_seq;
}

int
_get_flattened_seq_size(asdl_seq *seqs)
{
    int size = 0;
    for (int i = 0, l = asdl_seq_LEN(seqs); i < l; i++) {
        asdl_seq *inner_seq = asdl_seq_GET(seqs, i);

        // This following exclusion is needed, in order to correctly
        // handle the void pointers generated by the CONSTRUCTOR
        // function above.
        if (asdl_seq_GET(inner_seq, 0) == (void *)1) {
            continue;
        }

        size += asdl_seq_LEN(inner_seq);
    }
    return size;
}

/* Flattens an asdl_seq* of asdl_seq*s */
asdl_seq *
seq_flatten(Parser *p, asdl_seq *seqs)
{
    int flattened_seq_size = _get_flattened_seq_size(seqs);
    asdl_seq *flattened_seq = _Py_asdl_seq_new(flattened_seq_size, p->arena);
    if (!flattened_seq) {
        return NULL;
    }

    int flattened_seq_idx = 0;
    for (int i = 0, l = asdl_seq_LEN(seqs); i < l; i++) {
        asdl_seq *inner_seq = asdl_seq_GET(seqs, i);

        // This following exclusion is needed, in order to correctly
        // handle the void pointers generated by the CONSTRUCTOR
        // function above.
        if (asdl_seq_GET(inner_seq, 0) == (void *)1) {
            continue;
        }

        for (int j = 0, li = asdl_seq_LEN(inner_seq); j < li; j++) {
            asdl_seq_SET(flattened_seq, flattened_seq_idx++, asdl_seq_GET(inner_seq, j));
        }
    }
    assert(flattened_seq_idx == flattened_seq_size);

    return flattened_seq;
}

/* Creates a new name of the form <first_name>.<second_name> */
expr_ty
join_names_with_dot(Parser *p, expr_ty first_name, expr_ty second_name)
{
    PyObject *first_identifier = first_name->v.Name.id;
    PyObject *second_identifier = second_name->v.Name.id;

    if (PyUnicode_READY(first_identifier) == -1) {
        return NULL;
    }
    if (PyUnicode_READY(second_identifier) == -1) {
        return NULL;
    }
    const char *first_str = PyUnicode_AsUTF8(first_identifier);
    if (!first_str) {
        return NULL;
    }
    const char *second_str = PyUnicode_AsUTF8(second_identifier);
    if (!second_str) {
        return NULL;
    }
    ssize_t len = strlen(first_str) + strlen(second_str) + 1;  // +1 for the dot

    PyObject *str = PyBytes_FromStringAndSize(NULL, len);
    if (!str) {
        return NULL;
    }

    char *s = PyBytes_AS_STRING(str);
    if (!s) {
        return NULL;
    }

    strcpy(s, first_str);
    s += strlen(first_str);
    *s++ = '.';
    strcpy(s, second_str);
    s += strlen(second_str);
    *s = '\0';

    PyObject *uni = PyUnicode_DecodeUTF8(PyBytes_AS_STRING(str), PyBytes_GET_SIZE(str), NULL);
    Py_DECREF(str);
    if (!uni) {
        return NULL;
    }
    PyUnicode_InternInPlace(&uni);
    if (PyArena_AddPyObject(p->arena, uni) < 0) {
        Py_DECREF(uni);
        return NULL;
    }

    return _Py_Name(uni, Load, EXTRA_EXPR(first_name, second_name));
}

/* Counts the total number of dots in seq's tokens */
int
seq_count_dots(asdl_seq *seq)
{
    int number_of_dots = 0;
    for (int i = 0, l = asdl_seq_LEN(seq); i < l; i++) {
        Token *current_expr = asdl_seq_GET(seq, i);
        if (current_expr->type == ELLIPSIS) {
            number_of_dots += 3;
        }
        else if (current_expr->type == DOT) {
            number_of_dots += 1;
        }
        else {
            return -1;
        }
    }

    return number_of_dots;
}

/* Creates an alias with '*' as the identifier name */
alias_ty
alias_for_star(Parser *p)
{
    PyObject *str = PyUnicode_InternFromString("*");
    if (!str) {
        return NULL;
    }
    if (PyArena_AddPyObject(p->arena, str) < 0) {
        Py_DECREF(str);
        return NULL;
    }
    return alias(str, NULL, p->arena);
}

/* Returns the first element of seq or previous if seq is empty */
void *
seq_get_head(void *previous, asdl_seq *seq)
{
    if (asdl_seq_LEN(seq) == 0) {
        return previous;
    }
    return asdl_seq_GET(seq, 0);
}

/* Returns the last element of seq or previous if seq is empty */
void *
seq_get_tail(void *previous, asdl_seq *seq)
{
    if (asdl_seq_LEN(seq) == 0) {
        return previous;
    }
    return asdl_seq_GET(seq, asdl_seq_LEN(seq) - 1);
}

/* Creates a new asdl_seq* with the identifiers of all the names in seq */
asdl_seq *
map_names_to_ids(Parser *p, asdl_seq *seq)
{
    int len = asdl_seq_LEN(seq);
    asdl_seq *new_seq = _Py_asdl_seq_new(len, p->arena);
    if (!new_seq) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        expr_ty e = asdl_seq_GET(seq, i);
        asdl_seq_SET(new_seq, i, e->v.Name.id);
    }
    return new_seq;
}

/* Constructs a CmpopExprPair */
CmpopExprPair *
cmpop_expr_pair(Parser *p, cmpop_ty cmpop, expr_ty expr)
{
    CmpopExprPair *a = PyArena_Malloc(p->arena, sizeof(CmpopExprPair));
    if (!a) {
        return NULL;
    }
    a->cmpop = cmpop;
    a->expr = expr;
    return a;
}

asdl_int_seq *
_get_cmpops(Parser *p, asdl_seq *seq)
{
    int len = asdl_seq_LEN(seq);
    asdl_int_seq *new_seq = _Py_asdl_int_seq_new(len, p->arena);
    if (!new_seq) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        CmpopExprPair *pair = asdl_seq_GET(seq, i);
        asdl_seq_SET(new_seq, i, pair->cmpop);
    }
    return new_seq;
}

asdl_seq *
_get_exprs(Parser *p, asdl_seq *seq)
{
    int len = asdl_seq_LEN(seq);
    asdl_seq *new_seq = _Py_asdl_seq_new(len, p->arena);
    if (!new_seq) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        CmpopExprPair *pair = asdl_seq_GET(seq, i);
        asdl_seq_SET(new_seq, i, pair->expr);
    }
    return new_seq;
}

/* Wrapper for _Py_Compare, so that the call in the grammar stays concise */
expr_ty
Pegen_Compare(Parser *p, expr_ty expr, asdl_seq *pairs)
{
    return _Py_Compare(expr, _get_cmpops(p, pairs), _get_exprs(p, pairs),
                       EXTRA_EXPR(expr, ((CmpopExprPair *)seq_get_tail(NULL, pairs))->expr));
}

/* Creates an asdl_seq* where all the elements have been changed to have ctx as context */
static asdl_seq *
_set_seq_context(Parser *p, asdl_seq *seq, expr_context_ty ctx)
{
    if (!seq) {
        return NULL;
    }

    int len = asdl_seq_LEN(seq);
    asdl_seq *new_seq = _Py_asdl_seq_new(len, p->arena);
    if (!new_seq) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        expr_ty e = asdl_seq_GET(seq, i);
        asdl_seq_SET(new_seq, i, set_expr_context(p, e, ctx));
    }
    return new_seq;
}

static expr_ty
_set_name_context(Parser *p, expr_ty e, expr_context_ty ctx)
{
    return _Py_Name(e->v.Name.id, ctx, EXTRA_EXPR(e, e));
}

static expr_ty
_set_tuple_context(Parser *p, expr_ty e, expr_context_ty ctx)
{
    return _Py_Tuple(_set_seq_context(p, e->v.Tuple.elts, ctx), ctx, EXTRA_EXPR(e, e));
}

static expr_ty
_set_list_context(Parser *p, expr_ty e, expr_context_ty ctx)
{
    return _Py_List(_set_seq_context(p, e->v.List.elts, ctx), ctx, EXTRA_EXPR(e, e));
}

static expr_ty
_set_subscript_context(Parser *p, expr_ty e, expr_context_ty ctx)
{
    return _Py_Subscript(e->v.Subscript.value, e->v.Subscript.slice, ctx, EXTRA_EXPR(e, e));
}

static expr_ty
_set_attribute_context(Parser *p, expr_ty e, expr_context_ty ctx)
{
    return _Py_Attribute(e->v.Attribute.value, e->v.Attribute.attr, ctx, EXTRA_EXPR(e, e));
}

expr_ty
_set_starred_context(Parser *p, expr_ty e, expr_context_ty ctx)
{
    return _Py_Starred(set_expr_context(p, e->v.Starred.value, ctx), ctx, EXTRA_EXPR(e, e));
}

/* Receives a expr_ty and creates the appropiate node for assignment targets */
expr_ty
construct_assign_target(Parser *p, expr_ty node)
{
    if (!node) {
        return NULL;
    }

    switch (node->kind) {
        case Tuple_kind:
            if (asdl_seq_LEN(node->v.Tuple.elts) != 1) {
                PyErr_Format(PyExc_SyntaxError,
                             "Only single target (not tuple) can be annotated");
                // TODO: We need to return a dummy here because we don't have a way to
                // correctly buble up exceptions for now.
                return _Py_Name(_create_dummy_identifier(p), Store, EXTRA_EXPR(node, node));
            }
            return asdl_seq_GET(node->v.Tuple.elts, 0);
        case List_kind:
            PyErr_Format(PyExc_SyntaxError, "Only single target (not list) can be annotated");
            // TODO: We need to return a dummy here because we don't have a way to correctly
            // buble up exceptions for now.
            return _Py_Name(_create_dummy_identifier(p), Store, EXTRA_EXPR(node, node));
        default:
            return node;
    }
}

/* Creates an `expr_ty` equivalent to `expr` but with `ctx` as context */
expr_ty
set_expr_context(Parser *p, expr_ty expr, expr_context_ty ctx)
{
    if (!expr) {
        return NULL;
    }

    expr_ty new = NULL;
    switch (expr->kind) {
        case Name_kind:
            new = _set_name_context(p, expr, ctx);
            break;
        case Tuple_kind:
            new = _set_tuple_context(p, expr, ctx);
            break;
        case List_kind:
            new = _set_list_context(p, expr, ctx);
            break;
        case Subscript_kind:
            new = _set_subscript_context(p, expr, ctx);
            break;
        case Attribute_kind:
            new = _set_attribute_context(p, expr, ctx);
            break;
        case Starred_kind:
            new = _set_starred_context(p, expr, ctx);
            break;
        default:
            new = expr;
    }
    return new;
}

/* Constructs a KeyValuePair that is used when parsing a dict's key value pairs */
KeyValuePair *
key_value_pair(Parser *p, expr_ty key, expr_ty value)
{
    KeyValuePair *a = PyArena_Malloc(p->arena, sizeof(KeyValuePair));
    if (!a) {
        return NULL;
    }
    a->key = key;
    a->value = value;
    return a;
}

/* Constructs a NameDefaultPair */
NameDefaultPair *
name_default_pair(Parser *p, arg_ty arg, expr_ty value)
{
    NameDefaultPair *a = PyArena_Malloc(p->arena, sizeof(NameDefaultPair));
    if (!a) {
        return NULL;
    }
    a->arg = arg;
    a->value = value;
    return a;
}

/* Extracts all keys from an asdl_seq* of KeyValuePair*'s */
asdl_seq *
get_keys(Parser *p, asdl_seq *seq)
{
    int len = asdl_seq_LEN(seq);
    asdl_seq *new_seq = _Py_asdl_seq_new(len, p->arena);
    if (!new_seq) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        KeyValuePair *pair = asdl_seq_GET(seq, i);
        asdl_seq_SET(new_seq, i, pair->key);
    }
    return new_seq;
}

/* Extracts all values from an asdl_seq* of KeyValuePair*'s */
asdl_seq *
get_values(Parser *p, asdl_seq *seq)
{
    int len = asdl_seq_LEN(seq);
    asdl_seq *new_seq = _Py_asdl_seq_new(len, p->arena);
    if (!new_seq) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        KeyValuePair *pair = asdl_seq_GET(seq, i);
        asdl_seq_SET(new_seq, i, pair->value);
    }
    return new_seq;
}

/* Constructs a SlashWithDefault */
SlashWithDefault *
slash_with_default(Parser *p, asdl_seq *plain_names, asdl_seq *names_with_defaults)
{
    SlashWithDefault *a = PyArena_Malloc(p->arena, sizeof(SlashWithDefault));
    if (!a) {
        return NULL;
    }
    a->plain_names = plain_names;
    a->names_with_defaults = names_with_defaults;
    return a;
}

/* Constructs a StarEtc */
StarEtc *
star_etc(Parser *p, arg_ty vararg, asdl_seq *kwonlyargs, arg_ty kwarg)
{
    StarEtc *a = PyArena_Malloc(p->arena, sizeof(StarEtc));
    if (!a) {
        return NULL;
    }
    a->vararg = vararg;
    a->kwonlyargs = kwonlyargs;
    a->kwarg = kwarg;
    return a;
}

asdl_seq *
_join_seqs(Parser *p, asdl_seq *a, asdl_seq *b)
{
    int first_len = asdl_seq_LEN(a);
    int second_len = asdl_seq_LEN(b);
    asdl_seq *new_seq = _Py_asdl_seq_new(first_len + second_len, p->arena);
    if (!new_seq) {
        return NULL;
    }

    int k = 0;
    for (int i = 0; i < first_len; i++) {
        asdl_seq_SET(new_seq, k++, asdl_seq_GET(a, i));
    }
    for (int i = 0; i < second_len; i++) {
        asdl_seq_SET(new_seq, k++, asdl_seq_GET(b, i));
    }

    return new_seq;
}

asdl_seq *
_get_names(Parser *p, asdl_seq *names_with_defaults)
{
    int len = asdl_seq_LEN(names_with_defaults);
    asdl_seq *seq = _Py_asdl_seq_new(len, p->arena);
    if (!seq) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        NameDefaultPair *pair = asdl_seq_GET(names_with_defaults, i);
        asdl_seq_SET(seq, i, pair->arg);
    }
    return seq;
}

asdl_seq *
_get_defaults(Parser *p, asdl_seq *names_with_defaults)
{
    int len = asdl_seq_LEN(names_with_defaults);
    asdl_seq *seq = _Py_asdl_seq_new(len, p->arena);
    if (!seq) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        NameDefaultPair *pair = asdl_seq_GET(names_with_defaults, i);
        asdl_seq_SET(seq, i, pair->value);
    }
    return seq;
}

/* Constructs an arguments_ty object out of all the parsed constructs in the parameters rule */
arguments_ty
make_arguments(Parser *p, asdl_seq *slash_without_default,
               SlashWithDefault *slash_with_default, asdl_seq *plain_names,
               asdl_seq *names_with_default, StarEtc *star_etc)
{
    asdl_seq *posonlyargs;
    if (slash_without_default != NULL) {
        posonlyargs = slash_without_default;
    }
    else if (slash_with_default != NULL) {
        asdl_seq *slash_with_default_names =
            _get_names(p, slash_with_default->names_with_defaults);
        if (!slash_with_default_names) {
            return NULL;
        }
        posonlyargs = _join_seqs(p, slash_with_default->plain_names, slash_with_default_names);
        if (!posonlyargs) {
            return NULL;
        }
    }
    else {
        posonlyargs = _Py_asdl_seq_new(0, p->arena);
        if (!posonlyargs) {
            return NULL;
        }
    }

    asdl_seq *posargs;
    if (plain_names != NULL && names_with_default != NULL) {
        asdl_seq *names_with_default_names = _get_names(p, names_with_default);
        if (!names_with_default_names) {
            return NULL;
        }
        posargs = _join_seqs(p, plain_names, names_with_default_names);
        if (!posargs) {
            return NULL;
        }
    }
    else if (plain_names == NULL && names_with_default != NULL) {
        posargs = _get_names(p, names_with_default);
        if (!posargs) {
            return NULL;
        }
    }
    else if (plain_names != NULL && names_with_default == NULL) {
        posargs = plain_names;
    }
    else {
        posargs = _Py_asdl_seq_new(0, p->arena);
        if (!posargs) {
            return NULL;
        }
    }

    asdl_seq *posdefaults;
    if (slash_with_default != NULL && names_with_default != NULL) {
        asdl_seq *slash_with_default_values =
            _get_defaults(p, slash_with_default->names_with_defaults);
        if (!slash_with_default_values) {
            return NULL;
        }
        asdl_seq *names_with_default_values = _get_defaults(p, names_with_default);
        if (!names_with_default_values) {
            return NULL;
        }
        posdefaults = _join_seqs(p, slash_with_default_values, names_with_default_values);
        if (!posdefaults) {
            return NULL;
        }
    }
    else if (slash_with_default == NULL && names_with_default != NULL) {
        posdefaults = _get_defaults(p, names_with_default);
        if (!posdefaults) {
            return NULL;
        }
    }
    else if (slash_with_default != NULL && names_with_default == NULL) {
        posdefaults = _get_defaults(p, slash_with_default->names_with_defaults);
        if (!posdefaults) {
            return NULL;
        }
    }
    else {
        posdefaults = _Py_asdl_seq_new(0, p->arena);
        if (!posdefaults) {
            return NULL;
        }
    }

    arg_ty vararg = NULL;
    if (star_etc != NULL && star_etc->vararg != NULL) {
        vararg = star_etc->vararg;
    }

    asdl_seq *kwonlyargs;
    if (star_etc != NULL && star_etc->kwonlyargs != NULL) {
        kwonlyargs = _get_names(p, star_etc->kwonlyargs);
        if (!kwonlyargs) {
            return NULL;
        }
    }
    else {
        kwonlyargs = _Py_asdl_seq_new(0, p->arena);
        if (!kwonlyargs) {
            return NULL;
        }
    }

    asdl_seq *kwdefaults;
    if (star_etc != NULL && star_etc->kwonlyargs != NULL) {
        kwdefaults = _get_defaults(p, star_etc->kwonlyargs);
        if (!kwdefaults) {
            return NULL;
        }
    }
    else {
        kwdefaults = _Py_asdl_seq_new(0, p->arena);
        if (!kwdefaults) {
            return NULL;
        }
    }

    arg_ty kwarg = NULL;
    if (star_etc != NULL && star_etc->kwarg != NULL) {
        kwarg = star_etc->kwarg;
    }

    return _Py_arguments(posonlyargs, posargs, vararg, kwonlyargs, kwdefaults, kwarg,
                         posdefaults, p->arena);
}

/* Constructs an empty arguments_ty object, that gets used when a function accepts no
 * arguments. */
arguments_ty
empty_arguments(Parser *p)
{
    asdl_seq *posonlyargs = _Py_asdl_seq_new(0, p->arena);
    if (!posonlyargs) {
        return NULL;
    }
    asdl_seq *posargs = _Py_asdl_seq_new(0, p->arena);
    if (!posargs) {
        return NULL;
    }
    asdl_seq *posdefaults = _Py_asdl_seq_new(0, p->arena);
    if (!posdefaults) {
        return NULL;
    }
    asdl_seq *kwonlyargs = _Py_asdl_seq_new(0, p->arena);
    if (!kwonlyargs) {
        return NULL;
    }
    asdl_seq *kwdefaults = _Py_asdl_seq_new(0, p->arena);
    if (!kwdefaults) {
        return NULL;
    }

    return _Py_arguments(posonlyargs, posargs, NULL, kwonlyargs, kwdefaults, NULL, kwdefaults,
                         p->arena);
}

/* Encapsulates the value of an operator_ty into an AugOperator struct */
AugOperator *
augoperator(Parser *p, operator_ty kind)
{
    AugOperator *a = PyArena_Malloc(p->arena, sizeof(AugOperator));
    if (!a) {
        return NULL;
    }
    a->kind = kind;
    return a;
}

/* Construct a FunctionDef equivalent to function_def, but with decorators */
stmt_ty
function_def_decorators(Parser *p, asdl_seq *decorators, stmt_ty function_def)
{
    return _Py_FunctionDef(function_def->v.FunctionDef.name, function_def->v.FunctionDef.args,
                           function_def->v.FunctionDef.body, decorators,
                           function_def->v.FunctionDef.returns,
                           function_def->v.FunctionDef.type_comment, function_def->lineno,
                           function_def->col_offset, function_def->end_lineno,
                           function_def->end_col_offset, p->arena);
}

/* Construct a ClassDef equivalent to class_def, but with decorators */
stmt_ty
class_def_decorators(Parser *p, asdl_seq *decorators, stmt_ty class_def)
{
    return _Py_ClassDef(class_def->v.ClassDef.name, class_def->v.ClassDef.bases,
                        class_def->v.ClassDef.keywords, class_def->v.ClassDef.body, decorators,
                        class_def->lineno, class_def->col_offset, class_def->end_lineno,
                        class_def->end_col_offset, p->arena);
}

/* Construct a KeywordOrStarred */
KeywordOrStarred *
keyword_or_starred(Parser *p, void *element, int is_keyword)
{
    KeywordOrStarred *a = PyArena_Malloc(p->arena, sizeof(KeywordOrStarred));
    if (!a) {
        return NULL;
    }
    a->element = element;
    a->is_keyword = is_keyword;
    return a;
}

/* Get the number of starred expressions in an asdl_seq* of KeywordOrStarred*s */
static int
_seq_number_of_starred_exprs(asdl_seq *seq)
{
    int n = 0;
    for (int i = 0, l = asdl_seq_LEN(seq); i < l; i++) {
        KeywordOrStarred *k = asdl_seq_GET(seq, i);
        if (!k->is_keyword) {
            n++;
        }
    }
    return n;
}

/* Extract the starred expressions of an asdl_seq* of KeywordOrStarred*s */
asdl_seq *
seq_extract_starred_exprs(Parser *p, asdl_seq *kwargs)
{
    int new_len = _seq_number_of_starred_exprs(kwargs);
    if (new_len == 0) {
        return NULL;
    }
    asdl_seq *new_seq = _Py_asdl_seq_new(new_len, p->arena);
    if (!new_seq) {
        return NULL;
    }

    int idx = 0;
    for (int i = 0, len = asdl_seq_LEN(kwargs); i < len; i++) {
        KeywordOrStarred *k = asdl_seq_GET(kwargs, i);
        if (!k->is_keyword) {
            asdl_seq_SET(new_seq, idx++, k->element);
        }
    }
    return new_seq;
}

/* Return a new asdl_seq* with only the keywords in kwargs */
asdl_seq *
seq_delete_starred_exprs(Parser *p, asdl_seq *kwargs)
{
    int len = asdl_seq_LEN(kwargs);
    int new_len = len - _seq_number_of_starred_exprs(kwargs);
    if (new_len == 0) {
        return NULL;
    }
    asdl_seq *new_seq = _Py_asdl_seq_new(new_len, p->arena);
    if (!new_seq) {
        return NULL;
    }

    int idx = 0;
    for (int i = 0; i < len; i++) {
        KeywordOrStarred *k = asdl_seq_GET(kwargs, i);
        if (k->is_keyword) {
            asdl_seq_SET(new_seq, idx++, k->element);
        }
    }
    return new_seq;
}

//// STRING HANDLING FUNCTIONS ////

// These functions are ported directly from Python/ast.c with some modifications
// to account for the use of "Parser *p", the fact that don't have parser nodes
// to pass around and the usage of some specialized APIs present only in this
// file (like "raise_syntax_error").

static int
warn_invalid_escape_sequence(Parser *p, unsigned char first_invalid_escape_char)
{
    PyObject *msg =
        PyUnicode_FromFormat("invalid escape sequence \\%c", first_invalid_escape_char);
    if (msg == NULL) {
        return -1;
    }
    if (PyErr_WarnExplicitObject(PyExc_DeprecationWarning, msg, p->tok->filename,
                                 p->tok->lineno, NULL, NULL) < 0) {
        if (PyErr_ExceptionMatches(PyExc_DeprecationWarning)) {
            /* Replace the DeprecationWarning exception with a SyntaxError
               to get a more accurate error report */
            PyErr_Clear();
            raise_syntax_error(p, "invalid escape sequence \\%c", first_invalid_escape_char);
        }
        Py_DECREF(msg);
        return -1;
    }
    Py_DECREF(msg);
    return 0;
}

static PyObject *
decode_utf8(const char **sPtr, const char *end)
{
    const char *s, *t;
    t = s = *sPtr;
    while (s < end && (*s & 0x80)) {
        s++;
    }
    *sPtr = s;
    return PyUnicode_DecodeUTF8(t, s - t, NULL);
}

static PyObject *
decode_unicode_with_escapes(Parser *parser, const char *s, size_t len)
{
    PyObject *v, *u;
    char *buf;
    char *p;
    const char *end;

    /* check for integer overflow */
    if (len > SIZE_MAX / 6) {
        return NULL;
    }
    /* "ä" (2 bytes) may become "\U000000E4" (10 bytes), or 1:5
       "\ä" (3 bytes) may become "\u005c\U000000E4" (16 bytes), or ~1:6 */
    u = PyBytes_FromStringAndSize((char *)NULL, len * 6);
    if (u == NULL) {
        return NULL;
    }
    p = buf = PyBytes_AsString(u);
    end = s + len;
    while (s < end) {
        if (*s == '\\') {
            *p++ = *s++;
            if (s >= end || *s & 0x80) {
                strcpy(p, "u005c");
                p += 5;
                if (s >= end) {
                    break;
                }
            }
        }
        if (*s & 0x80) {
            PyObject *w;
            int kind;
            void *data;
            Py_ssize_t len, i;
            w = decode_utf8(&s, end);
            if (w == NULL) {
                Py_DECREF(u);
                return NULL;
            }
            kind = PyUnicode_KIND(w);
            data = PyUnicode_DATA(w);
            len = PyUnicode_GET_LENGTH(w);
            for (i = 0; i < len; i++) {
                Py_UCS4 chr = PyUnicode_READ(kind, data, i);
                sprintf(p, "\\U%08x", chr);
                p += 10;
            }
            /* Should be impossible to overflow */
            assert(p - buf <= PyBytes_GET_SIZE(u));
            Py_DECREF(w);
        }
        else {
            *p++ = *s++;
        }
    }
    len = p - buf;
    s = buf;

    const char *first_invalid_escape;
    v = _PyUnicode_DecodeUnicodeEscape(s, len, NULL, &first_invalid_escape);

    if (v != NULL && first_invalid_escape != NULL) {
        if (warn_invalid_escape_sequence(parser, *first_invalid_escape) < 0) {
            /* We have not decref u before because first_invalid_escape points
               inside u. */
            Py_XDECREF(u);
            Py_DECREF(v);
            return NULL;
        }
    }
    Py_XDECREF(u);
    return v;
}

static PyObject *
decode_bytes_with_escapes(Parser *p, const char *s, Py_ssize_t len)
{
    const char *first_invalid_escape;
    PyObject *result = _PyBytes_DecodeEscape(s, len, NULL, 0, NULL, &first_invalid_escape);
    if (result == NULL) {
        return NULL;
    }

    if (first_invalid_escape != NULL) {
        if (warn_invalid_escape_sequence(p, *first_invalid_escape) < 0) {
            Py_DECREF(result);
            return NULL;
        }
    }
    return result;
}

/* s must include the bracketing quote characters, and r, b, u,
   &/or f prefixes (if any), and embedded escape sequences (if any).
   parsestr parses it, and sets *result to decoded Python string object.
   If the string is an f-string, set *fstr and *fstrlen to the unparsed
   string object.  Return 0 if no errors occurred.  */
static int
parsestr(Parser *p, const char *s, int *bytesmode, int *rawmode, PyObject **result,
         const char **fstr, Py_ssize_t *fstrlen)
{
    size_t len;
    int quote = Py_CHARMASK(*s);
    int fmode = 0;
    *bytesmode = 0;
    *rawmode = 0;
    *result = NULL;
    *fstr = NULL;
    if (Py_ISALPHA(quote)) {
        while (!*bytesmode || !*rawmode) {
            if (quote == 'b' || quote == 'B') {
                quote = *++s;
                *bytesmode = 1;
            }
            else if (quote == 'u' || quote == 'U') {
                quote = *++s;
            }
            else if (quote == 'r' || quote == 'R') {
                quote = *++s;
                *rawmode = 1;
            }
            else if (quote == 'f' || quote == 'F') {
                quote = *++s;
                fmode = 1;
            }
            else {
                break;
            }
        }
    }

    if (fmode && *bytesmode) {
        PyErr_BadInternalCall();
        return -1;
    }
    if (quote != '\'' && quote != '\"') {
        PyErr_BadInternalCall();
        return -1;
    }
    /* Skip the leading quote char. */
    s++;
    len = strlen(s);
    if (len > INT_MAX) {
        PyErr_SetString(PyExc_OverflowError, "string to parse is too long");
        return -1;
    }
    if (s[--len] != quote) {
        /* Last quote char must match the first. */
        PyErr_BadInternalCall();
        return -1;
    }
    if (len >= 4 && s[0] == quote && s[1] == quote) {
        /* A triple quoted string. We've already skipped one quote at
           the start and one at the end of the string. Now skip the
           two at the start. */
        s += 2;
        len -= 2;
        /* And check that the last two match. */
        if (s[--len] != quote || s[--len] != quote) {
            PyErr_BadInternalCall();
            return -1;
        }
    }

    if (fmode) {
        /* Just return the bytes. The caller will parse the resulting
           string. */
        *fstr = s;
        *fstrlen = len;
        return 0;
    }

    /* Not an f-string. */
    /* Avoid invoking escape decoding routines if possible. */
    *rawmode = *rawmode || strchr(s, '\\') == NULL;
    if (*bytesmode) {
        /* Disallow non-ASCII characters. */
        const char *ch;
        for (ch = s; *ch; ch++) {
            if (Py_CHARMASK(*ch) >= 0x80) {
                raise_syntax_error(p,
                                   "bytes can only contain ASCII "
                                   "literal characters.");
                return -1;
            }
        }
        if (*rawmode) {
            *result = PyBytes_FromStringAndSize(s, len);
        }
        else {
            *result = decode_bytes_with_escapes(p, s, len);
        }
    }
    else {
        if (*rawmode) {
            *result = PyUnicode_DecodeUTF8Stateful(s, len, NULL, NULL);
        }
        else {
            *result = decode_unicode_with_escapes(p, s, len);
        }
    }
    return *result == NULL ? -1 : 0;
}



// FSTRING STUFF

/* /1* Shift locations for the given node and all its children by adding `lineno` */
/*    and `col_offset` to existing locations. *1/ */
/* static void fstring_shift_node_locations(node *n, int lineno, int col_offset) */
/* { */
/*     n->n_col_offset = n->n_col_offset + col_offset; */
/*     n->n_end_col_offset = n->n_end_col_offset + col_offset; */
/*     for (int i = 0; i < NCH(n); ++i) { */
/*         if (n->n_lineno && n->n_lineno < CHILD(n, i)->n_lineno) { */
/*             /1* Shifting column offsets unnecessary if there's been newlines. *1/ */
/*             col_offset = 0; */
/*         } */
/*         fstring_shift_node_locations(CHILD(n, i), lineno, col_offset); */
/*     } */
/*     n->n_lineno = n->n_lineno + lineno; */
/*     n->n_end_lineno = n->n_end_lineno + lineno; */
/* } */

/* Fix locations for the given node and its children.

   `parent` is the enclosing node.
   `n` is the node which locations are going to be fixed relative to parent.
   `expr_str` is the child node's string representation, including braces.
*/
/* static void */
/* fstring_fix_node_location(const node *parent, node *n, char *expr_str) */
/* { */
/*     char *substr = NULL; */
/*     char *start; */
/*     int lines = LINENO(parent) - 1; */
/*     int cols = parent->n_col_offset; */
/*     /1* Find the full fstring to fix location information in `n`. *1/ */
/*     while (parent && parent->n_type != STRING) */
/*         parent = parent->n_child; */
/*     if (parent && parent->n_str) { */
/*         substr = strstr(parent->n_str, expr_str); */
/*         if (substr) { */
/*             start = substr; */
/*             while (start > parent->n_str) { */
/*                 if (start[0] == '\n') */
/*                     break; */
/*                 start--; */
/*             } */
/*             cols += (int)(substr - start); */
/*             /1* adjust the start based on the number of newlines encountered */
/*                before the f-string expression *1/ */
/*             for (char* p = parent->n_str; p < substr; p++) { */
/*                 if (*p == '\n') { */
/*                     lines++; */
/*                 } */
/*             } */
/*         } */
/*     } */
/*     fstring_shift_node_locations(n, lines, cols); */
/* } */


/* Compile this expression in to an expr_ty.  Add parens around the
   expression, in order to allow leading spaces in the expression. */
static expr_ty
fstring_compile_expr(Parser *p, const char *expr_start, const char *expr_end)
{
    mod_ty mod;
    char *str;
    Py_ssize_t len;
    const char *s;

    assert(expr_end >= expr_start);
    assert(*(expr_start-1) == '{');
    assert(*expr_end == '}' || *expr_end == '!' || *expr_end == ':' ||
           *expr_end == '=');

    /* If the substring is all whitespace, it's an error.  We need to catch this
       here, and not when we call PyParser_SimpleParseStringFlagsFilename,
       because turning the expression '' in to '()' would go from being invalid
       to valid. */
    for (s = expr_start; s != expr_end; s++) {
        char c = *s;
        /* The Python parser ignores only the following whitespace
           characters (\r already is converted to \n). */
        if (!(c == ' ' || c == '\t' || c == '\n' || c == '\f')) {
            break;
        }
    }
    if (s == expr_end) {
        raise_syntax_error(p, "f-string: empty expression not allowed");
        return NULL;
    }

    len = expr_end - expr_start;
    /* Allocate 3 extra bytes: open paren, close paren, null byte. */
    str = PyMem_RawMalloc(len + 3);
    if (str == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    str[0] = '(';
    memcpy(str+1, expr_start, len);
    str[len+1] = ')';
    str[len+2] = 0;

    struct tok_state* tok = PyTokenizer_FromString(str, 1);
    if (tok == NULL) {
        return NULL;
    }
    mod_ty (*the_start_rule)(Parser*) = p->start_rule_func;

    Parser *p2 = PyMem_Malloc(sizeof(Parser));
    if (p2 == NULL) {
        PyErr_Format(PyExc_MemoryError, "Out of memory for Parser");
        goto exit;
    }
    p2->tok = tok;
    p2->keywords = p->keywords;
    p2->n_keyword_lists = p->n_keyword_lists;
    p2->tokens = PyMem_Malloc(sizeof(Token *));
    if (!p2->tokens) {
        PyErr_Format(PyExc_MemoryError, "Out of memory for tokens");
        goto exit;
    }
    p2->tokens[0] = PyMem_Malloc(sizeof(Token));
    memset(p2->tokens[0], '\0', sizeof(Token));
    p2->mark = 0;
    p2->fill = 0;
    p2->size = 1;
    p2->arena = p->arena;
    if (fill_token(p2) < 0) {
        goto exit;
    }
    PyErr_Clear();
    mod = the_start_rule(p2);


    PyTokenizer_Free(tok);

    /* Reuse str to find the correct column offset. */
    str[0] = '{';
    str[len+1] = '}';
    if (!mod) {
        return NULL;
    }

exit:
    for (int i = 0; i < p2->size; i++) {
        PyMem_Free(p2->tokens[i]);
    }
    PyMem_Free(p2->tokens);
    PyMem_Free(p2);
    if (mod == NULL) {
        return NULL;
    }
    if (asdl_seq_LEN(mod->v.Module.body) != 1) {
        raise_syntax_error(p, "f-string: invalid expression");
    }
    return expr->v.Subscript.value;
}

/* Return -1 on error.

   Return 0 if we reached the end of the literal.

   Return 1 if we haven't reached the end of the literal, but we want
   the caller to process the literal up to this point. Used for
   doubled braces.
*/
static int
fstring_find_literal(Parser *p, const char **str, const char *end, int raw,
                     PyObject **literal, int recurse_lvl)
{
    /* Get any literal string. It ends when we hit an un-doubled left
       brace (which isn't part of a unicode name escape such as
       "\N{EULER CONSTANT}"), or the end of the string. */

    const char *s = *str;
    const char *literal_start = s;
    int result = 0;

    assert(*literal == NULL);
    while (s < end) {
        char ch = *s++;
        if (!raw && ch == '\\' && s < end) {
            ch = *s++;
            if (ch == 'N') {
                if (s < end && *s++ == '{') {
                    while (s < end && *s++ != '}') {
                    }
                    continue;
                }
                break;
            }
            if (ch == '{' && warn_invalid_escape_sequence(p, ch) < 0) {
                return -1;
            }
        }
        if (ch == '{' || ch == '}') {
            /* Check for doubled braces, but only at the top level. If
               we checked at every level, then f'{0:{3}}' would fail
               with the two closing braces. */
            if (recurse_lvl == 0) {
                if (s < end && *s == ch) {
                    /* We're going to tell the caller that the literal ends
                       here, but that they should continue scanning. But also
                       skip over the second brace when we resume scanning. */
                    *str = s + 1;
                    result = 1;
                    goto done;
                }

                /* Where a single '{' is the start of a new expression, a
                   single '}' is not allowed. */
                if (ch == '}') {
                    *str = s - 1;
                    raise_syntax_error(p, "f-string: single '}' is not allowed");
                    return -1;
                }
            }
            /* We're either at a '{', which means we're starting another
               expression; or a '}', which means we're at the end of this
               f-string (for a nested format_spec). */
            s--;
            break;
        }
    }
    *str = s;
    assert(s <= end);
    assert(s == end || *s == '{' || *s == '}');
done:
    if (literal_start != s) {
        if (raw)
            *literal = PyUnicode_DecodeUTF8Stateful(literal_start,
                                                    s - literal_start,
                                                    NULL, NULL);
        else
            *literal = decode_unicode_with_escapes(p, literal_start,
                                                   s - literal_start);
        if (!*literal)
            return -1;
    }
    return result;
}

/* Forward declaration because parsing is recursive. */
static expr_ty
fstring_parse(Parser *p, const char **str, const char *end, int raw, int recurse_lvl,
              Token* t);

/* Parse the f-string at *str, ending at end.  We know *str starts an
   expression (so it must be a '{'). Returns the FormattedValue node, which
   includes the expression, conversion character, format_spec expression, and
   optionally the text of the expression (if = is used).

   Note that I don't do a perfect job here: I don't make sure that a
   closing brace doesn't match an opening paren, for example. It
   doesn't need to error on all invalid expressions, just correctly
   find the end of all valid ones. Any errors inside the expression
   will be caught when we parse it later.

   *expression is set to the expression.  For an '=' "debug" expression,
   *expr_text is set to the debug text (the original text of the expression,
   including the '=' and any whitespace around it, as a string object).  If
   not a debug expression, *expr_text set to NULL. */
static int
fstring_find_expr(Parser *p, const char **str, const char *end, int raw, int recurse_lvl,
                  PyObject **expr_text, expr_ty *expression)
{
    /* Return -1 on error, else 0. */

    const char *expr_start;
    const char *expr_end;
    expr_ty simple_expression;
    expr_ty format_spec = NULL; /* Optional format specifier. */
    int conversion = -1; /* The conversion char.  Use default if not
                            specified, or !r if using = and no format
                            spec. */

    /* 0 if we're not in a string, else the quote char we're trying to
       match (single or double quote). */
    char quote_char = 0;

    /* If we're inside a string, 1=normal, 3=triple-quoted. */
    int string_type = 0;

    /* Keep track of nesting level for braces/parens/brackets in
       expressions. */
    Py_ssize_t nested_depth = 0;
    char parenstack[MAXLEVEL];

    *expr_text = NULL;

    /* Can only nest one level deep. */
    if (recurse_lvl >= 2) {
        raise_syntax_error(p, "f-string: expressions nested too deeply");
        goto error;
    }

    /* The first char must be a left brace, or we wouldn't have gotten
       here. Skip over it. */
    assert(**str == '{');
    *str += 1;

    expr_start = *str;
    for (; *str < end; (*str)++) {
        char ch;

        /* Loop invariants. */
        assert(nested_depth >= 0);
        assert(*str >= expr_start && *str < end);
        if (quote_char)
            assert(string_type == 1 || string_type == 3);
        else
            assert(string_type == 0);

        ch = **str;
        /* Nowhere inside an expression is a backslash allowed. */
        if (ch == '\\') {
            /* Error: can't include a backslash character, inside
               parens or strings or not. */
            raise_syntax_error(p,
                      "f-string expression part "
                      "cannot include a backslash");
            goto error;
        }
        if (quote_char) {
            /* We're inside a string. See if we're at the end. */
            /* This code needs to implement the same non-error logic
               as tok_get from tokenizer.c, at the letter_quote
               label. To actually share that code would be a
               nightmare. But, it's unlikely to change and is small,
               so duplicate it here. Note we don't need to catch all
               of the errors, since they'll be caught when parsing the
               expression. We just need to match the non-error
               cases. Thus we can ignore \n in single-quoted strings,
               for example. Or non-terminated strings. */
            if (ch == quote_char) {
                /* Does this match the string_type (single or triple
                   quoted)? */
                if (string_type == 3) {
                    if (*str+2 < end && *(*str+1) == ch && *(*str+2) == ch) {
                        /* We're at the end of a triple quoted string. */
                        *str += 2;
                        string_type = 0;
                        quote_char = 0;
                        continue;
                    }
                } else {
                    /* We're at the end of a normal string. */
                    quote_char = 0;
                    string_type = 0;
                    continue;
                }
            }
        } else if (ch == '\'' || ch == '"') {
            /* Is this a triple quoted string? */
            if (*str+2 < end && *(*str+1) == ch && *(*str+2) == ch) {
                string_type = 3;
                *str += 2;
            } else {
                /* Start of a normal string. */
                string_type = 1;
            }
            /* Start looking for the end of the string. */
            quote_char = ch;
        } else if (ch == '[' || ch == '{' || ch == '(') {
            if (nested_depth >= MAXLEVEL) {
                raise_syntax_error(p, "f-string: too many nested parenthesis");
                goto error;
            }
            parenstack[nested_depth] = ch;
            nested_depth++;
        } else if (ch == '#') {
            /* Error: can't include a comment character, inside parens
               or not. */
            raise_syntax_error(p, "f-string expression part cannot include '#'");
            goto error;
        } else if (nested_depth == 0 &&
                   (ch == '!' || ch == ':' || ch == '}' ||
                    ch == '=' || ch == '>' || ch == '<')) {
            /* See if there's a next character. */
            if (*str+1 < end) {
                char next = *(*str+1);

                /* For "!=". since '=' is not an allowed conversion character,
                   nothing is lost in this test. */
                if ((ch == '!' && next == '=') ||   /* != */
                    (ch == '=' && next == '=') ||   /* == */
                    (ch == '<' && next == '=') ||   /* <= */
                    (ch == '>' && next == '=')      /* >= */
                    ) {
                    *str += 1;
                    continue;
                }
                /* Don't get out of the loop for these, if they're single
                   chars (not part of 2-char tokens). If by themselves, they
                   don't end an expression (unlike say '!'). */
                if (ch == '>' || ch == '<') {
                    continue;
                }
            }

            /* Normal way out of this loop. */
            break;
        } else if (ch == ']' || ch == '}' || ch == ')') {
            if (!nested_depth) {
                raise_syntax_error(p, "f-string: unmatched '%c'", ch);
                goto error;
            }
            nested_depth--;
            int opening = parenstack[nested_depth];
            if (!((opening == '(' && ch == ')') ||
                  (opening == '[' && ch == ']') ||
                  (opening == '{' && ch == '}')))
            {
                raise_syntax_error(p,
                          "f-string: closing parenthesis '%c' "
                          "does not match opening parenthesis '%c'",
                          ch, opening);
                goto error;
            }
        } else {
            /* Just consume this char and loop around. */
        }
    }
    expr_end = *str;
    /* If we leave this loop in a string or with mismatched parens, we
       don't care. We'll get a syntax error when compiling the
       expression. But, we can produce a better error message, so
       let's just do that.*/
    if (quote_char) {
        raise_syntax_error(p, "f-string: unterminated string");
        goto error;
    }
    if (nested_depth) {
        int opening = parenstack[nested_depth - 1];
        raise_syntax_error(p, "f-string: unmatched '%c'", opening);
        goto error;
    }

    if (*str >= end)
        goto unexpected_end_of_string;

    /* Compile the expression as soon as possible, so we show errors
       related to the expression before errors related to the
       conversion or format_spec. */
    simple_expression = fstring_compile_expr(p, expr_start, expr_end);
    if (!simple_expression)
        goto error;

    /* Check for =, which puts the text value of the expression in
       expr_text. */
    if (**str == '=') {
        *str += 1;

        /* Skip over ASCII whitespace.  No need to test for end of string
           here, since we know there's at least a trailing quote somewhere
           ahead. */
        while (Py_ISSPACE(**str)) {
            *str += 1;
        }

        /* Set *expr_text to the text of the expression. */
        *expr_text = PyUnicode_FromStringAndSize(expr_start, *str-expr_start);
        if (!*expr_text) {
            goto error;
        }
    }

    /* Check for a conversion char, if present. */
    if (**str == '!') {
        *str += 1;
        if (*str >= end)
            goto unexpected_end_of_string;

        conversion = **str;
        *str += 1;

        /* Validate the conversion. */
        if (!(conversion == 's' || conversion == 'r' || conversion == 'a')) {
            raise_syntax_error(p,
                      "f-string: invalid conversion character: "
                      "expected 's', 'r', or 'a'");
            goto error;
        }

    }

    /* Check for the format spec, if present. */
    if (*str >= end)
        goto unexpected_end_of_string;
    if (**str == ':') {
        *str += 1;
        if (*str >= end)
            goto unexpected_end_of_string;

        /* Parse the format spec. */
        format_spec = fstring_parse(p, str, end, raw, recurse_lvl+1, t);
        if (!format_spec)
            goto error;
    }

    if (*str >= end || **str != '}')
        goto unexpected_end_of_string;

    /* We're at a right brace. Consume it. */
    assert(*str < end);
    assert(**str == '}');
    *str += 1;

    /* If we're in = mode (detected by non-NULL expr_text), and have no format
       spec and no explicit conversion, set the conversion to 'r'. */
    if (*expr_text && format_spec == NULL && conversion == -1) {
        conversion = 'r';
    }

    /* And now create the FormattedValue node that represents this
       entire expression with the conversion and format spec. */
    //TODO: Fix this
    *expression = FormattedValue(simple_expression, conversion,
                                 format_spec, 1, 1, 1, 1, p->arena);
    if (!*expression)
        goto error;

    return 0;

unexpected_end_of_string:
    raise_syntax_error(p, "f-string: expecting '}'");
    /* Falls through to error. */

error:
    Py_XDECREF(*expr_text);
    return -1;

}

/* Return -1 on error.

   Return 0 if we have a literal (possible zero length) and an
   expression (zero length if at the end of the string.

   Return 1 if we have a literal, but no expression, and we want the
   caller to call us again. This is used to deal with doubled
   braces.

   When called multiple times on the string 'a{{b{0}c', this function
   will return:

   1. the literal 'a{' with no expression, and a return value
      of 1. Despite the fact that there's no expression, the return
      value of 1 means we're not finished yet.

   2. the literal 'b' and the expression '0', with a return value of
      0. The fact that there's an expression means we're not finished.

   3. literal 'c' with no expression and a return value of 0. The
      combination of the return value of 0 with no expression means
      we're finished.
*/
static int
fstring_find_literal_and_expr(Parser *p, const char **str, const char *end, int raw,
                              int recurse_lvl, PyObject **literal,
                              PyObject **expr_text, expr_ty *expression,
                              Token *t)
{
    int result;

    assert(*literal == NULL && *expression == NULL);

    /* Get any literal string. */
    result = fstring_find_literal(p, str, end, raw, literal, recurse_lvl);
    if (result < 0)
        goto error;

    assert(result == 0 || result == 1);

    if (result == 1)
        /* We have a literal, but don't look at the expression. */
        return 1;

    if (*str >= end || **str == '}')
        /* We're at the end of the string or the end of a nested
           f-string: no expression. The top-level error case where we
           expect to be at the end of the string but we're at a '}' is
           handled later. */
        return 0;

    /* We must now be the start of an expression, on a '{'. */
    assert(**str == '{');

    if (fstring_find_expr(p, str, end, raw, recurse_lvl, expr_text,
                          expression) < 0)
        goto error;

    return 0;

error:
    Py_CLEAR(*literal);
    return -1;
}




#define EXPRLIST_N_CACHED  64

typedef struct {
    /* Incrementally build an array of expr_ty, so be used in an
       asdl_seq. Cache some small but reasonably sized number of
       expr_ty's, and then after that start dynamically allocating,
       doubling the number allocated each time. Note that the f-string
       f'{0}a{1}' contains 3 expr_ty's: 2 FormattedValue's, and one
       Constant for the literal 'a'. So you add expr_ty's about twice as
       fast as you add expressions in an f-string. */

    Py_ssize_t allocated;  /* Number we've allocated. */
    Py_ssize_t size;       /* Number we've used. */
    expr_ty    *p;         /* Pointer to the memory we're actually
                              using. Will point to 'data' until we
                              start dynamically allocating. */
    expr_ty    data[EXPRLIST_N_CACHED];
} ExprList;

#ifdef NDEBUG
#define ExprList_check_invariants(l)
#else
static void
ExprList_check_invariants(ExprList *l)
{
    /* Check our invariants. Make sure this object is "live", and
       hasn't been deallocated. */
    assert(l->size >= 0);
    assert(l->p != NULL);
    if (l->size <= EXPRLIST_N_CACHED)
        assert(l->data == l->p);
}
#endif

static void
ExprList_Init(ExprList *l)
{
    l->allocated = EXPRLIST_N_CACHED;
    l->size = 0;

    /* Until we start allocating dynamically, p points to data. */
    l->p = l->data;

    ExprList_check_invariants(l);
}

static int
ExprList_Append(ExprList *l, expr_ty exp)
{
    ExprList_check_invariants(l);
    if (l->size >= l->allocated) {
        /* We need to alloc (or realloc) the memory. */
        Py_ssize_t new_size = l->allocated * 2;

        /* See if we've ever allocated anything dynamically. */
        if (l->p == l->data) {
            Py_ssize_t i;
            /* We're still using the cached data. Switch to
               alloc-ing. */
            l->p = PyMem_RawMalloc(sizeof(expr_ty) * new_size);
            if (!l->p)
                return -1;
            /* Copy the cached data into the new buffer. */
            for (i = 0; i < l->size; i++)
                l->p[i] = l->data[i];
        } else {
            /* Just realloc. */
            expr_ty *tmp = PyMem_RawRealloc(l->p, sizeof(expr_ty) * new_size);
            if (!tmp) {
                PyMem_RawFree(l->p);
                l->p = NULL;
                return -1;
            }
            l->p = tmp;
        }

        l->allocated = new_size;
        assert(l->allocated == 2 * l->size);
    }

    l->p[l->size++] = exp;

    ExprList_check_invariants(l);
    return 0;
}

static void
ExprList_Dealloc(ExprList *l)
{
    ExprList_check_invariants(l);

    /* If there's been an error, or we've never dynamically allocated,
       do nothing. */
    if (!l->p || l->p == l->data) {
        /* Do nothing. */
    } else {
        /* We have dynamically allocated. Free the memory. */
        PyMem_RawFree(l->p);
    }
    l->p = NULL;
    l->size = -1;
}

static asdl_seq *
ExprList_Finish(ExprList *l, PyArena *arena)
{
    asdl_seq *seq;

    ExprList_check_invariants(l);

    /* Allocate the asdl_seq and copy the expressions in to it. */
    seq = _Py_asdl_seq_new(l->size, arena);
    if (seq) {
        Py_ssize_t i;
        for (i = 0; i < l->size; i++)
            asdl_seq_SET(seq, i, l->p[i]);
    }
    ExprList_Dealloc(l);
    return seq;
}



/* The FstringParser is designed to add a mix of strings and
   f-strings, and concat them together as needed. Ultimately, it
   generates an expr_ty. */
typedef struct {
    PyObject *last_str;
    ExprList expr_list;
    int fmode;
} FstringParser;

#ifdef NDEBUG
#define FstringParser_check_invariants(state)
#else
static void
FstringParser_check_invariants(FstringParser *state)
{
    if (state->last_str)
        assert(PyUnicode_CheckExact(state->last_str));
    ExprList_check_invariants(&state->expr_list);
}
#endif

static void
FstringParser_Init(FstringParser *state)
{
    state->last_str = NULL;
    state->fmode = 0;
    ExprList_Init(&state->expr_list);
    FstringParser_check_invariants(state);
}

static void
FstringParser_Dealloc(FstringParser *state)
{
    FstringParser_check_invariants(state);

    Py_XDECREF(state->last_str);
    ExprList_Dealloc(&state->expr_list);
}

/* Make a Constant node, but decref the PyUnicode object being added. */
static expr_ty
make_str_node_and_del(Parser *p, PyObject **str, Token* t)
{
    PyObject *s = *str;
    PyObject *kind = NULL;
    *str = NULL;
    assert(PyUnicode_CheckExact(s));
    if (PyArena_AddPyObject(p->arena, s) < 0) {
        Py_DECREF(s);
        return NULL;
    }
    //TODO: Check this logic with the kind
    //
    const char* the_str = PyUnicode_AsUTF8(s);
    if (the_str && the_str[0] == 'u') {
        kind = new_identifier(p, "u");
    }

    if (kind == NULL && PyErr_Occurred()) {
        return NULL;
    }

    return Constant(s, kind, t->lineno, t->col_offset, t->end_lineno,
                    t->end_col_offset, p->arena);

}


/* Add a non-f-string (that is, a regular literal string). str is
   decref'd. */
static int
FstringParser_ConcatAndDel(FstringParser *state, PyObject *str)
{
    FstringParser_check_invariants(state);

    assert(PyUnicode_CheckExact(str));

    if (PyUnicode_GET_LENGTH(str) == 0) {
        Py_DECREF(str);
        return 0;
    }

    if (!state->last_str) {
        /* We didn't have a string before, so just remember this one. */
        state->last_str = str;
    } else {
        /* Concatenate this with the previous string. */
        PyUnicode_AppendAndDel(&state->last_str, str);
        if (!state->last_str)
            return -1;
    }
    FstringParser_check_invariants(state);
    return 0;
}

/* Parse an f-string. The f-string is in *str to end, with no
   'f' or quotes. */
static int
FstringParser_ConcatFstring(Parser *p, FstringParser *state, const char **str,
                            const char *end, int raw, int recurse_lvl, Token* t)
{
    FstringParser_check_invariants(state);
    state->fmode = 1;

    /* Parse the f-string. */
    while (1) {
        PyObject *literal = NULL;
        PyObject *expr_text = NULL;
        expr_ty expression = NULL;

        /* If there's a zero length literal in front of the
           expression, literal will be NULL. If we're at the end of
           the f-string, expression will be NULL (unless result == 1,
           see below). */
        int result = fstring_find_literal_and_expr(p, str, end, raw, recurse_lvl,
                                                   &literal, &expr_text,
                                                   &expression);
        if (result < 0)
            return -1;

        /* Add the literal, if any. */
        if (literal && FstringParser_ConcatAndDel(state, literal) < 0) {
            Py_XDECREF(expr_text);
            return -1;
        }
        /* Add the expr_text, if any. */
        if (expr_text && FstringParser_ConcatAndDel(state, expr_text) < 0) {
            return -1;
        }

        /* We've dealt with the literal and expr_text, their ownership has
           been transferred to the state object.  Don't look at them again. */

        /* See if we should just loop around to get the next literal
           and expression, while ignoring the expression this
           time. This is used for un-doubling braces, as an
           optimization. */
        if (result == 1)
            continue;

        if (!expression)
            /* We're done with this f-string. */
            break;

        /* We know we have an expression. Convert any existing string
           to a Constant node. */
        if (!state->last_str) {
            /* Do nothing. No previous literal. */
        } else {
            /* Convert the existing last_str literal to a Constant node. */
            expr_ty str = make_str_node_and_del(p, &state->last_str, t);
            if (!str || ExprList_Append(&state->expr_list, str) < 0)
                return -1;
        }

        if (ExprList_Append(&state->expr_list, expression) < 0)
            return -1;
    }

    /* If recurse_lvl is zero, then we must be at the end of the
       string. Otherwise, we must be at a right brace. */

    if (recurse_lvl == 0 && *str < end-1) {
        raise_syntax_error(p, "f-string: unexpected end of string");
        return -1;
    }
    if (recurse_lvl != 0 && **str != '}') {
        raise_syntax_error(p, "f-string: expecting '}'");
        return -1;
    }

    FstringParser_check_invariants(state);
    return 0;
}

/* Convert the partial state reflected in last_str and expr_list to an
   expr_ty. The expr_ty can be a Constant, or a JoinedStr. */
static expr_ty
FstringParser_Finish(Parser *p, FstringParser *state, Token* t)
{
    asdl_seq *seq;

    FstringParser_check_invariants(state);

    /* If we're just a constant string with no expressions, return
       that. */
    if (!state->fmode) {
        assert(!state->expr_list.size);
        if (!state->last_str) {
            /* Create a zero length string. */
            state->last_str = PyUnicode_FromStringAndSize(NULL, 0);
            if (!state->last_str)
                goto error;
        }
        return make_str_node_and_del(p, &state->last_str, t);
    }

    /* Create a Constant node out of last_str, if needed. It will be the
       last node in our expression list. */
    if (state->last_str) {
        expr_ty str = make_str_node_and_del(p, &state->last_str, t);
        if (!str || ExprList_Append(&state->expr_list, str) < 0)
            goto error;
    }
    /* This has already been freed. */
    assert(state->last_str == NULL);

    seq = ExprList_Finish(&state->expr_list, p->arena);
    if (!seq)
        goto error;

    return _Py_JoinedStr(seq, t->lineno, t->col_offset, t->end_lineno,
                    t->end_col_offset, p->arena);



error:
    FstringParser_Dealloc(state);
    return NULL;
}

/* Given an f-string (with no 'f' or quotes) that's in *str and ends
   at end, parse it into an expr_ty.  Return NULL on error.  Adjust
   str to point past the parsed portion. */
static expr_ty
fstring_parse(Parser *p, const char **str, const char *end, int raw,
              int recurse_lvl, Token* t)
{
    FstringParser state;

    FstringParser_Init(&state);
    if (FstringParser_ConcatFstring(p, &state, str, end, raw, recurse_lvl, t) < 0) {
        FstringParser_Dealloc(&state);
        return NULL;
    }

    return FstringParser_Finish(p, &state, t);
}

expr_ty
concatenate_strings(Parser *p, asdl_seq *strings)
{
    int len = asdl_seq_LEN(strings);
    assert(len > 0);

    expr_ty first = asdl_seq_GET(strings, 0);
    expr_ty last = asdl_seq_GET(strings, len - 1);

    int bytesmode = 0;
    PyObject *u_kind = NULL;
    int kind_unicode = 0;
    PyObject *final_str = NULL;
    return first;

    // TODO(Pablo): This assert will fail because we are returning
    // JoinedStr now here: We need to add logic to join f-strings!
    assert(first->kind == Constant_kind);
    if (PyBytes_CheckExact(first->v.Constant.value)) {
        final_str = PyBytes_FromString("");
    }
    else {
        final_str = PyUnicode_FromString("");
    }
    if (final_str == NULL) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        expr_ty cons = asdl_seq_GET(strings, i);
        assert(cons->kind == Constant_kind);
        PyObject *s = cons->v.Constant.value;
        int this_bytesmode = PyBytes_CheckExact(s);

        if (i != 0 && bytesmode != this_bytesmode) {
            raise_syntax_error(p, "cannot mix bytes and nonbytes literals");
            goto error;
        }

        bytesmode = this_bytesmode;

        if (bytesmode) {
            PyBytes_Concat(&final_str, s);
            if (!final_str) {
                goto error;
            }
        }
        else {
            kind_unicode |= (cons->v.Constant.kind != NULL);
            PyUnicode_Append(&final_str, s);
            if (!final_str) {
                goto error;
            }
        }
    }

    if (kind_unicode) {
        // TODO: Intern this string when we decide how we will
        // handle static constants in the module.
        u_kind = new_identifier(p, "u");
    }
    if (PyArena_AddPyObject(p->arena, final_str) < 0) {
        Py_DECREF(final_str);
        return NULL;
    }
    return _Py_Constant(final_str, u_kind, EXTRA_EXPR(first, last));

error:
    Py_XDECREF(final_str);
    return NULL;
}

expr_ty
string_token(Parser *p)
{
    Token *t = expect_token(p, STRING);

    if (t == NULL) {
        return NULL;
    }

    char *the_str = PyBytes_AsString(t->bytes);
    if (the_str == NULL) {
        return NULL;
    }

    PyObject *s;
    int this_rawmode = 0;
    int bytesmode = 0;
    const char *fstr;
    Py_ssize_t fstrlen = -1; /* Silence a compiler warning. */
    if (parsestr(p, the_str, &bytesmode, &this_rawmode, &s, &fstr, &fstrlen) != 0) {
        return NULL;
    }

    /* Check if it has a 'u' prefix */
    int kind_unicode = 0;
    if (the_str[0] == 'u') {
        assert(!bytesmode);
        kind_unicode = 1;
    }

    if (fstr != NULL) {
        /* We are parsing an f-string. */
        assert(s == NULL && !bytesmode);

        FstringParser state;
        FstringParser_Init(&state);
        int result = FstringParser_ConcatFstring(p, &state, &fstr, fstr+fstrlen, this_rawmode, 0, t);
        return FstringParser_Finish(p, &state, t);
    }

    /* A string or byte string. */
    assert(s != NULL && fstr == NULL);
    assert(bytesmode ? PyBytes_CheckExact(s) : PyUnicode_CheckExact(s));

    PyObject *final_str = s;
    if (PyArena_AddPyObject(p->arena, final_str) < 0) {
        goto error;
    }

    PyObject *u_kind = NULL;
    if (!bytesmode && kind_unicode) {
        // TODO: Intern this string when we decide how we will
        // handle static constants in the module.
        u_kind = new_identifier(p, "u");
        if (u_kind == NULL) {
            goto error;
        }
    }
    return Constant(final_str, u_kind, t->lineno, t->col_offset, t->end_lineno,
                    t->end_col_offset, p->arena);

error:
    Py_DECREF(final_str);
    return NULL;
}
