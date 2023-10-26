package types

import (
	"encoding/hex"
	"testing"
)

func TestIsValidName(t *testing.T) {
	// Test case 1: Valid name with letters and spaces
	t.Run("Test case 1", func(t *testing.T) {
		result := isValidName("John Doe")
		expected := true
		if result != expected {
			t.Errorf("For name 'John Doe', expected %v, but got %v", expected, result)
		}
	})

	// Test case 2: Valid name with only letters (no spaces)
	t.Run("Test case 2", func(t *testing.T) {
		result := isValidName("Alice")
		expected := true
		if result != expected {
			t.Errorf("For name 'Alice', expected %v, but got %v", expected, result)
		}
	})

	// Test case 3: Valid name with leading and trailing spaces
	t.Run("Test case 3", func(t *testing.T) {
		result := isValidName("  Bob Smith  ")
		expected := true
		if result != expected {
			t.Errorf("For name '  Bob Smith  ', expected %v, but got %v", expected, result)
		}
	})

	// Test case 4: Name with digits
	t.Run("Test case 4", func(t *testing.T) {
		result := isValidName("Jane123")
		expected := false
		if result != expected {
			t.Errorf("For name 'Jane123', expected %v, but got %v", expected, result)
		}
	})

	// Test case 5: Name with special characters
	t.Run("Test case 5", func(t *testing.T) {
		result := isValidName("Mary@Doe")
		expected := false
		if result != expected {
			t.Errorf("For name 'Mary@Doe', expected %v, but got %v", expected, result)
		}
	})

	// Test case 6: Empty name
	t.Run("Test case 6", func(t *testing.T) {
		result := isValidName("")
		expected := false
		if result != expected {
			t.Errorf("For an empty name, expected %v, but got %v", expected, result)
		}
	})

	// Test case 7: Name with newline character
	t.Run("Test case 7", func(t *testing.T) {
		result := isValidName("Eve\nJohnson")
		expected := false
		if result != expected {
			t.Errorf("For name 'Eve\nJohnson', expected %v, but got %v", expected, result)
		}
	})
}

func TestIsValidJSON(t *testing.T) {
	// Test case 1: Valid JSON
	t.Run("Valid JSON", func(t *testing.T) {
		jsonStr := `{"name":"John","age":30}`
		result := isValidJSON(jsonStr)
		expected := true
		if result != expected {
			t.Errorf("For JSON string '%s', expected %v, but got %v", jsonStr, expected, result)
		}
	})

	// Test case 2: Invalid JSON (missing closing bracket)
	t.Run("Invalid JSON (missing closing bracket)", func(t *testing.T) {
		jsonStr := `{"name":"Jane","age":25`
		result := isValidJSON(jsonStr)
		expected := false
		if result != expected {
			t.Errorf("For JSON string '%s', expected %v, but got %v", jsonStr, expected, result)
		}
	})

	// Test case 3: Invalid JSON (extra comma)
	t.Run("Invalid JSON (extra comma)", func(t *testing.T) {
		jsonStr := `{"name":"Bob","age":40,}`
		result := isValidJSON(jsonStr)
		expected := false
		if result != expected {
			t.Errorf("For JSON string '%s', expected %v, but got %v", jsonStr, expected, result)
		}
	})

	// Test case 4: Empty JSON object
	t.Run("Empty JSON object", func(t *testing.T) {
		jsonStr := `{}`
		result := isValidJSON(jsonStr)
		expected := true
		if result != expected {
			t.Errorf("For JSON string '%s', expected %v, but got %v", jsonStr, expected, result)
		}
	})

	// Test case 6: Empty string
	t.Run("Empty string", func(t *testing.T) {
		jsonStr := ``
		result := isValidJSON(jsonStr)
		expected := false // An empty string is not valid JSON.
		if result != expected {
			t.Errorf("For an empty string, expected %v, but got %v", expected, result)
		}
	})
}

func TestIsValidEmail(t *testing.T) {
	// Test case 1: Valid email
	t.Run("Valid email", func(t *testing.T) {
		result := isValidEmail("john.doe@example.com")
		expected := true
		if result != expected {
			t.Errorf("For email 'john.doe@example.com', expected %v, but got %v", expected, result)
		}
	})

	// Test case 2: Valid email with subdomain
	t.Run("Valid email with subdomain", func(t *testing.T) {
		result := isValidEmail("jane@sub.example.co.uk")
		expected := true
		if result != expected {
			t.Errorf("For email 'jane@sub.example.co.uk', expected %v, but got %v", expected, result)
		}
	})

	// Test case 3: Invalid email (missing @ symbol)
	t.Run("Invalid email (missing @ symbol)", func(t *testing.T) {
		result := isValidEmail("invalid.email.com")
		expected := false
		if result != expected {
			t.Errorf("For email 'invalid.email.com', expected %v, but got %v", expected, result)
		}
	})

	// Test case 4: Invalid email (special characters)
	t.Run("Invalid email (special characters)", func(t *testing.T) {
		result := isValidEmail("user@ex@mple.com")
		expected := false
		if result != expected {
			t.Errorf("For email 'user@ex@mple.com', expected %v, but got %v", expected, result)
		}
	})

	// Test case 5: Invalid email (no domain)
	t.Run("Invalid email (no domain)", func(t *testing.T) {
		result := isValidEmail("user@.com")
		expected := false
		if result != expected {
			t.Errorf("For email 'user@.com', expected %v, but got %v", expected, result)
		}
	})

	// Test case 6: Invalid email (no user)
	t.Run("Invalid email (no user)", func(t *testing.T) {
		result := isValidEmail("@example.com")
		expected := false
		if result != expected {
			t.Errorf("For email '@example.com', expected %v, but got %v", expected, result)
		}
	})

	// Test case 7: Empty string
	t.Run("Empty string", func(t *testing.T) {
		result := isValidEmail("")
		expected := false
		if result != expected {
			t.Errorf("For an empty string, expected %v, but got %v", expected, result)
		}
	})
}

func TestGenerateRandomID(t *testing.T) {
	t.Run("Test GenerateRandomID", func(t *testing.T) {
		randomID := generateRandomID()

		// Check if the generated ID is not empty
		if len(randomID) == 0 {
			t.Errorf("Generated random ID is empty.")
		}

		// Check if the generated ID is a hexadecimal string
		_, err := hex.DecodeString(randomID)
		if err != nil {
			t.Errorf("Generated random ID is not a valid hexadecimal string: %s", randomID)
		}
	})
}
