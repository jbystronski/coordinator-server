// package middleware

// import (
// 	"net/http"
// 	"strings"
// )

// func AuthorizeByToken(secret string, next http.Handler) http.Handler {
// 	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 		authHeader := r.Header.Get("Authorization")
// 		if !strings.HasPrefix(authHeader, "Bearer ") {
// 			http.Error(w, "Unauthorized access", http.StatusUnauthorized)
// 			return
// 		}

// 		token := strings.TrimPrefix(authHeader, "Bearer ")
// 		if token != secret {
// 			http.Error(w, "Forbidden", http.StatusForbidden)
// 			return
// 		}

// 		next.ServeHTTP(w, r)
// 	})
// }

package middleware

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

func AuthorizeByToken(secret string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if secret == "" {
			http.Error(w, "Server authentication is not configured", http.StatusInternalServerError)
			return
		}

		authHeader := r.Header.Get("Authorization")
		token, ok := strings.CutPrefix(authHeader, "Bearer ")
		if !ok || token == "" ||
			subtle.ConstantTimeCompare([]byte(token), []byte(secret)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}
