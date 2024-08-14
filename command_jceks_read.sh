# Extract the password using 'openssl'
password=$(openssl enc -d -aes-256-cbc -in "$keystore_file" -pass pass:"$keystore_password" | grep "$alias_name" | awk '{print $NF}')


============

keystore_file="your_keystore.jks"
keystore_password="your_keystore_password"
alias_name="your_alias"

# Extract the certificate in DER format
keytool -exportcert -alias "$alias_name" -keystore "$keystore_file" -storepass "$keystore_password" -rfc822=true -file cert.der


===============


# Path to your JCEKS keystore file
keystore_file="mykeystore.jceks"

# Keystore password (used to open the keystore)
keystore_password="keystore"

# Alias for the password entry
alias_name="myalias"

# Load the keystore
ks=$(keytool -list -keystore "$keystore_file" -storetype jceks -storepass "$keystore_password")
