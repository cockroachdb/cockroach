// GENERATED FILE DO NOT EDIT
import * as $protobuf from "protobufjs";

/**
 * Namespace cockroach.
 * @exports cockroach
 * @namespace
 */
export namespace cockroach {

    /**
     * Namespace ccl.
     * @exports cockroach.ccl
     * @namespace
     */
    namespace ccl {

        /**
         * Namespace storageccl.
         * @exports cockroach.ccl.storageccl
         * @namespace
         */
        namespace storageccl {

            /**
             * Namespace engineccl.
             * @exports cockroach.ccl.storageccl.engineccl
             * @namespace
             */
            namespace engineccl {

                /**
                 * Namespace enginepbccl.
                 * @exports cockroach.ccl.storageccl.engineccl.enginepbccl
                 * @namespace
                 */
                namespace enginepbccl {

                    /**
                     * EncryptionType enum.
                     * @name EncryptionType
                     * @memberof cockroach.ccl.storageccl.engineccl.enginepbccl
                     * @enum {number}
                     * @property {number} Plaintext=0 Plaintext value
                     * @property {number} AES128_CTR=1 AES128_CTR value
                     * @property {number} AES192_CTR=2 AES192_CTR value
                     * @property {number} AES256_CTR=3 AES256_CTR value
                     */
                    enum EncryptionType {
                        Plaintext = 0,
                        AES128_CTR = 1,
                        AES192_CTR = 2,
                        AES256_CTR = 3
                    }

                    type DataKeysRegistry$Properties = {
                        store_keys?: { [k: string]: cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties };
                        data_keys?: { [k: string]: cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties };
                        active_store_key?: string;
                        active_data_key?: string;
                    };

                    /**
                     * Constructs a new DataKeysRegistry.
                     * @exports cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry
                     * @constructor
                     * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry$Properties=} [properties] Properties to set
                     */
                    class DataKeysRegistry {

                        /**
                         * Constructs a new DataKeysRegistry.
                         * @exports cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry
                         * @constructor
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry$Properties=} [properties] Properties to set
                         */
                        constructor(properties?: cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry$Properties);

                        /**
                         * DataKeysRegistry store_keys.
                         * @type {Object.<string,cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties>}
                         */
                        public store_keys: { [k: string]: cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties };

                        /**
                         * DataKeysRegistry data_keys.
                         * @type {Object.<string,cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties>}
                         */
                        public data_keys: { [k: string]: cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties };

                        /**
                         * DataKeysRegistry active_store_key.
                         * @type {string}
                         */
                        public active_store_key: string;

                        /**
                         * DataKeysRegistry active_data_key.
                         * @type {string}
                         */
                        public active_data_key: string;

                        /**
                         * Creates a new DataKeysRegistry instance using the specified properties.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry$Properties=} [properties] Properties to set
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry} DataKeysRegistry instance
                         */
                        public static create(properties?: cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry$Properties): cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry;

                        /**
                         * Encodes the specified DataKeysRegistry message. Does not implicitly {@link cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry.verify|verify} messages.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry$Properties} message DataKeysRegistry message or plain object to encode
                         * @param {$protobuf.Writer} [writer] Writer to encode to
                         * @returns {$protobuf.Writer} Writer
                         */
                        public static encode(message: cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                        /**
                         * Encodes the specified DataKeysRegistry message, length delimited. Does not implicitly {@link cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry.verify|verify} messages.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry$Properties} message DataKeysRegistry message or plain object to encode
                         * @param {$protobuf.Writer} [writer] Writer to encode to
                         * @returns {$protobuf.Writer} Writer
                         */
                        public static encodeDelimited(message: cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                        /**
                         * Decodes a DataKeysRegistry message from the specified reader or buffer.
                         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                         * @param {number} [length] Message length if known beforehand
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry} DataKeysRegistry
                         * @throws {Error} If the payload is not a reader or valid buffer
                         * @throws {$protobuf.util.ProtocolError} If required fields are missing
                         */
                        public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry;

                        /**
                         * Decodes a DataKeysRegistry message from the specified reader or buffer, length delimited.
                         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry} DataKeysRegistry
                         * @throws {Error} If the payload is not a reader or valid buffer
                         * @throws {$protobuf.util.ProtocolError} If required fields are missing
                         */
                        public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry;

                        /**
                         * Verifies a DataKeysRegistry message.
                         * @param {Object.<string,*>} message Plain object to verify
                         * @returns {?string} `null` if valid, otherwise the reason why it is not
                         */
                        public static verify(message: { [k: string]: any }): string;

                        /**
                         * Creates a DataKeysRegistry message from a plain object. Also converts values to their respective internal types.
                         * @param {Object.<string,*>} object Plain object
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry} DataKeysRegistry
                         */
                        public static fromObject(object: { [k: string]: any }): cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry;

                        /**
                         * Creates a DataKeysRegistry message from a plain object. Also converts values to their respective internal types.
                         * This is an alias of {@link cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry.fromObject}.
                         * @function
                         * @param {Object.<string,*>} object Plain object
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry} DataKeysRegistry
                         */
                        public static from(object: { [k: string]: any }): cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry;

                        /**
                         * Creates a plain object from a DataKeysRegistry message. Also converts values to other types if specified.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry} message DataKeysRegistry
                         * @param {$protobuf.ConversionOptions} [options] Conversion options
                         * @returns {Object.<string,*>} Plain object
                         */
                        public static toObject(message: cockroach.ccl.storageccl.engineccl.enginepbccl.DataKeysRegistry, options?: $protobuf.ConversionOptions): { [k: string]: any };

                        /**
                         * Creates a plain object from this DataKeysRegistry message. Also converts values to other types if specified.
                         * @param {$protobuf.ConversionOptions} [options] Conversion options
                         * @returns {Object.<string,*>} Plain object
                         */
                        public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

                        /**
                         * Converts this DataKeysRegistry to JSON.
                         * @returns {Object.<string,*>} JSON object
                         */
                        public toJSON(): { [k: string]: any };
                    }

                    type KeyInfo$Properties = {
                        encryption_type?: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionType;
                        key_id?: string;
                        creation_time?: Long;
                        source?: string;
                        was_exposed?: boolean;
                        parent_key_id?: string;
                    };

                    /**
                     * Constructs a new KeyInfo.
                     * @exports cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo
                     * @constructor
                     * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties=} [properties] Properties to set
                     */
                    class KeyInfo {

                        /**
                         * Constructs a new KeyInfo.
                         * @exports cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo
                         * @constructor
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties=} [properties] Properties to set
                         */
                        constructor(properties?: cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties);

                        /**
                         * KeyInfo encryption_type.
                         * @type {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionType}
                         */
                        public encryption_type: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionType;

                        /**
                         * KeyInfo key_id.
                         * @type {string}
                         */
                        public key_id: string;

                        /**
                         * KeyInfo creation_time.
                         * @type {Long}
                         */
                        public creation_time: Long;

                        /**
                         * KeyInfo source.
                         * @type {string}
                         */
                        public source: string;

                        /**
                         * KeyInfo was_exposed.
                         * @type {boolean}
                         */
                        public was_exposed: boolean;

                        /**
                         * KeyInfo parent_key_id.
                         * @type {string}
                         */
                        public parent_key_id: string;

                        /**
                         * Creates a new KeyInfo instance using the specified properties.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties=} [properties] Properties to set
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo} KeyInfo instance
                         */
                        public static create(properties?: cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties): cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo;

                        /**
                         * Encodes the specified KeyInfo message. Does not implicitly {@link cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo.verify|verify} messages.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties} message KeyInfo message or plain object to encode
                         * @param {$protobuf.Writer} [writer] Writer to encode to
                         * @returns {$protobuf.Writer} Writer
                         */
                        public static encode(message: cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                        /**
                         * Encodes the specified KeyInfo message, length delimited. Does not implicitly {@link cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo.verify|verify} messages.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties} message KeyInfo message or plain object to encode
                         * @param {$protobuf.Writer} [writer] Writer to encode to
                         * @returns {$protobuf.Writer} Writer
                         */
                        public static encodeDelimited(message: cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                        /**
                         * Decodes a KeyInfo message from the specified reader or buffer.
                         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                         * @param {number} [length] Message length if known beforehand
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo} KeyInfo
                         * @throws {Error} If the payload is not a reader or valid buffer
                         * @throws {$protobuf.util.ProtocolError} If required fields are missing
                         */
                        public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo;

                        /**
                         * Decodes a KeyInfo message from the specified reader or buffer, length delimited.
                         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo} KeyInfo
                         * @throws {Error} If the payload is not a reader or valid buffer
                         * @throws {$protobuf.util.ProtocolError} If required fields are missing
                         */
                        public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo;

                        /**
                         * Verifies a KeyInfo message.
                         * @param {Object.<string,*>} message Plain object to verify
                         * @returns {?string} `null` if valid, otherwise the reason why it is not
                         */
                        public static verify(message: { [k: string]: any }): string;

                        /**
                         * Creates a KeyInfo message from a plain object. Also converts values to their respective internal types.
                         * @param {Object.<string,*>} object Plain object
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo} KeyInfo
                         */
                        public static fromObject(object: { [k: string]: any }): cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo;

                        /**
                         * Creates a KeyInfo message from a plain object. Also converts values to their respective internal types.
                         * This is an alias of {@link cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo.fromObject}.
                         * @function
                         * @param {Object.<string,*>} object Plain object
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo} KeyInfo
                         */
                        public static from(object: { [k: string]: any }): cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo;

                        /**
                         * Creates a plain object from a KeyInfo message. Also converts values to other types if specified.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo} message KeyInfo
                         * @param {$protobuf.ConversionOptions} [options] Conversion options
                         * @returns {Object.<string,*>} Plain object
                         */
                        public static toObject(message: cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo, options?: $protobuf.ConversionOptions): { [k: string]: any };

                        /**
                         * Creates a plain object from this KeyInfo message. Also converts values to other types if specified.
                         * @param {$protobuf.ConversionOptions} [options] Conversion options
                         * @returns {Object.<string,*>} Plain object
                         */
                        public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

                        /**
                         * Converts this KeyInfo to JSON.
                         * @returns {Object.<string,*>} JSON object
                         */
                        public toJSON(): { [k: string]: any };
                    }

                    type SecretKey$Properties = {
                        info?: cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties;
                        key?: Uint8Array;
                    };

                    /**
                     * Constructs a new SecretKey.
                     * @exports cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey
                     * @constructor
                     * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties=} [properties] Properties to set
                     */
                    class SecretKey {

                        /**
                         * Constructs a new SecretKey.
                         * @exports cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey
                         * @constructor
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties=} [properties] Properties to set
                         */
                        constructor(properties?: cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties);

                        /**
                         * SecretKey info.
                         * @type {(cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties|null)}
                         */
                        public info: (cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties|null);

                        /**
                         * SecretKey key.
                         * @type {Uint8Array}
                         */
                        public key: Uint8Array;

                        /**
                         * Creates a new SecretKey instance using the specified properties.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties=} [properties] Properties to set
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey} SecretKey instance
                         */
                        public static create(properties?: cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties): cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey;

                        /**
                         * Encodes the specified SecretKey message. Does not implicitly {@link cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey.verify|verify} messages.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties} message SecretKey message or plain object to encode
                         * @param {$protobuf.Writer} [writer] Writer to encode to
                         * @returns {$protobuf.Writer} Writer
                         */
                        public static encode(message: cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                        /**
                         * Encodes the specified SecretKey message, length delimited. Does not implicitly {@link cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey.verify|verify} messages.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties} message SecretKey message or plain object to encode
                         * @param {$protobuf.Writer} [writer] Writer to encode to
                         * @returns {$protobuf.Writer} Writer
                         */
                        public static encodeDelimited(message: cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                        /**
                         * Decodes a SecretKey message from the specified reader or buffer.
                         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                         * @param {number} [length] Message length if known beforehand
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey} SecretKey
                         * @throws {Error} If the payload is not a reader or valid buffer
                         * @throws {$protobuf.util.ProtocolError} If required fields are missing
                         */
                        public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey;

                        /**
                         * Decodes a SecretKey message from the specified reader or buffer, length delimited.
                         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey} SecretKey
                         * @throws {Error} If the payload is not a reader or valid buffer
                         * @throws {$protobuf.util.ProtocolError} If required fields are missing
                         */
                        public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey;

                        /**
                         * Verifies a SecretKey message.
                         * @param {Object.<string,*>} message Plain object to verify
                         * @returns {?string} `null` if valid, otherwise the reason why it is not
                         */
                        public static verify(message: { [k: string]: any }): string;

                        /**
                         * Creates a SecretKey message from a plain object. Also converts values to their respective internal types.
                         * @param {Object.<string,*>} object Plain object
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey} SecretKey
                         */
                        public static fromObject(object: { [k: string]: any }): cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey;

                        /**
                         * Creates a SecretKey message from a plain object. Also converts values to their respective internal types.
                         * This is an alias of {@link cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey.fromObject}.
                         * @function
                         * @param {Object.<string,*>} object Plain object
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey} SecretKey
                         */
                        public static from(object: { [k: string]: any }): cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey;

                        /**
                         * Creates a plain object from a SecretKey message. Also converts values to other types if specified.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey} message SecretKey
                         * @param {$protobuf.ConversionOptions} [options] Conversion options
                         * @returns {Object.<string,*>} Plain object
                         */
                        public static toObject(message: cockroach.ccl.storageccl.engineccl.enginepbccl.SecretKey, options?: $protobuf.ConversionOptions): { [k: string]: any };

                        /**
                         * Creates a plain object from this SecretKey message. Also converts values to other types if specified.
                         * @param {$protobuf.ConversionOptions} [options] Conversion options
                         * @returns {Object.<string,*>} Plain object
                         */
                        public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

                        /**
                         * Converts this SecretKey to JSON.
                         * @returns {Object.<string,*>} JSON object
                         */
                        public toJSON(): { [k: string]: any };
                    }

                    type EncryptionSettings$Properties = {
                        encryption_type?: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionType;
                        key_id?: string;
                        nonce?: Uint8Array;
                        counter?: number;
                    };

                    /**
                     * Constructs a new EncryptionSettings.
                     * @exports cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings
                     * @constructor
                     * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings$Properties=} [properties] Properties to set
                     */
                    class EncryptionSettings {

                        /**
                         * Constructs a new EncryptionSettings.
                         * @exports cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings
                         * @constructor
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings$Properties=} [properties] Properties to set
                         */
                        constructor(properties?: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings$Properties);

                        /**
                         * EncryptionSettings encryption_type.
                         * @type {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionType}
                         */
                        public encryption_type: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionType;

                        /**
                         * EncryptionSettings key_id.
                         * @type {string}
                         */
                        public key_id: string;

                        /**
                         * EncryptionSettings nonce.
                         * @type {Uint8Array}
                         */
                        public nonce: Uint8Array;

                        /**
                         * EncryptionSettings counter.
                         * @type {number}
                         */
                        public counter: number;

                        /**
                         * Creates a new EncryptionSettings instance using the specified properties.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings$Properties=} [properties] Properties to set
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings} EncryptionSettings instance
                         */
                        public static create(properties?: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings$Properties): cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings;

                        /**
                         * Encodes the specified EncryptionSettings message. Does not implicitly {@link cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings.verify|verify} messages.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings$Properties} message EncryptionSettings message or plain object to encode
                         * @param {$protobuf.Writer} [writer] Writer to encode to
                         * @returns {$protobuf.Writer} Writer
                         */
                        public static encode(message: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                        /**
                         * Encodes the specified EncryptionSettings message, length delimited. Does not implicitly {@link cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings.verify|verify} messages.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings$Properties} message EncryptionSettings message or plain object to encode
                         * @param {$protobuf.Writer} [writer] Writer to encode to
                         * @returns {$protobuf.Writer} Writer
                         */
                        public static encodeDelimited(message: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                        /**
                         * Decodes an EncryptionSettings message from the specified reader or buffer.
                         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                         * @param {number} [length] Message length if known beforehand
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings} EncryptionSettings
                         * @throws {Error} If the payload is not a reader or valid buffer
                         * @throws {$protobuf.util.ProtocolError} If required fields are missing
                         */
                        public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings;

                        /**
                         * Decodes an EncryptionSettings message from the specified reader or buffer, length delimited.
                         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings} EncryptionSettings
                         * @throws {Error} If the payload is not a reader or valid buffer
                         * @throws {$protobuf.util.ProtocolError} If required fields are missing
                         */
                        public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings;

                        /**
                         * Verifies an EncryptionSettings message.
                         * @param {Object.<string,*>} message Plain object to verify
                         * @returns {?string} `null` if valid, otherwise the reason why it is not
                         */
                        public static verify(message: { [k: string]: any }): string;

                        /**
                         * Creates an EncryptionSettings message from a plain object. Also converts values to their respective internal types.
                         * @param {Object.<string,*>} object Plain object
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings} EncryptionSettings
                         */
                        public static fromObject(object: { [k: string]: any }): cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings;

                        /**
                         * Creates an EncryptionSettings message from a plain object. Also converts values to their respective internal types.
                         * This is an alias of {@link cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings.fromObject}.
                         * @function
                         * @param {Object.<string,*>} object Plain object
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings} EncryptionSettings
                         */
                        public static from(object: { [k: string]: any }): cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings;

                        /**
                         * Creates a plain object from an EncryptionSettings message. Also converts values to other types if specified.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings} message EncryptionSettings
                         * @param {$protobuf.ConversionOptions} [options] Conversion options
                         * @returns {Object.<string,*>} Plain object
                         */
                        public static toObject(message: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionSettings, options?: $protobuf.ConversionOptions): { [k: string]: any };

                        /**
                         * Creates a plain object from this EncryptionSettings message. Also converts values to other types if specified.
                         * @param {$protobuf.ConversionOptions} [options] Conversion options
                         * @returns {Object.<string,*>} Plain object
                         */
                        public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

                        /**
                         * Converts this EncryptionSettings to JSON.
                         * @returns {Object.<string,*>} JSON object
                         */
                        public toJSON(): { [k: string]: any };
                    }

                    type EncryptionStatus$Properties = {
                        active_store_key?: cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties;
                        active_data_key?: cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties;
                    };

                    /**
                     * Constructs a new EncryptionStatus.
                     * @exports cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus
                     * @constructor
                     * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus$Properties=} [properties] Properties to set
                     */
                    class EncryptionStatus {

                        /**
                         * Constructs a new EncryptionStatus.
                         * @exports cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus
                         * @constructor
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus$Properties=} [properties] Properties to set
                         */
                        constructor(properties?: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus$Properties);

                        /**
                         * EncryptionStatus active_store_key.
                         * @type {(cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties|null)}
                         */
                        public active_store_key: (cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties|null);

                        /**
                         * EncryptionStatus active_data_key.
                         * @type {(cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties|null)}
                         */
                        public active_data_key: (cockroach.ccl.storageccl.engineccl.enginepbccl.KeyInfo$Properties|null);

                        /**
                         * Creates a new EncryptionStatus instance using the specified properties.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus$Properties=} [properties] Properties to set
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus} EncryptionStatus instance
                         */
                        public static create(properties?: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus$Properties): cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus;

                        /**
                         * Encodes the specified EncryptionStatus message. Does not implicitly {@link cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus.verify|verify} messages.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus$Properties} message EncryptionStatus message or plain object to encode
                         * @param {$protobuf.Writer} [writer] Writer to encode to
                         * @returns {$protobuf.Writer} Writer
                         */
                        public static encode(message: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                        /**
                         * Encodes the specified EncryptionStatus message, length delimited. Does not implicitly {@link cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus.verify|verify} messages.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus$Properties} message EncryptionStatus message or plain object to encode
                         * @param {$protobuf.Writer} [writer] Writer to encode to
                         * @returns {$protobuf.Writer} Writer
                         */
                        public static encodeDelimited(message: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                        /**
                         * Decodes an EncryptionStatus message from the specified reader or buffer.
                         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                         * @param {number} [length] Message length if known beforehand
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus} EncryptionStatus
                         * @throws {Error} If the payload is not a reader or valid buffer
                         * @throws {$protobuf.util.ProtocolError} If required fields are missing
                         */
                        public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus;

                        /**
                         * Decodes an EncryptionStatus message from the specified reader or buffer, length delimited.
                         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus} EncryptionStatus
                         * @throws {Error} If the payload is not a reader or valid buffer
                         * @throws {$protobuf.util.ProtocolError} If required fields are missing
                         */
                        public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus;

                        /**
                         * Verifies an EncryptionStatus message.
                         * @param {Object.<string,*>} message Plain object to verify
                         * @returns {?string} `null` if valid, otherwise the reason why it is not
                         */
                        public static verify(message: { [k: string]: any }): string;

                        /**
                         * Creates an EncryptionStatus message from a plain object. Also converts values to their respective internal types.
                         * @param {Object.<string,*>} object Plain object
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus} EncryptionStatus
                         */
                        public static fromObject(object: { [k: string]: any }): cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus;

                        /**
                         * Creates an EncryptionStatus message from a plain object. Also converts values to their respective internal types.
                         * This is an alias of {@link cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus.fromObject}.
                         * @function
                         * @param {Object.<string,*>} object Plain object
                         * @returns {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus} EncryptionStatus
                         */
                        public static from(object: { [k: string]: any }): cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus;

                        /**
                         * Creates a plain object from an EncryptionStatus message. Also converts values to other types if specified.
                         * @param {cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus} message EncryptionStatus
                         * @param {$protobuf.ConversionOptions} [options] Conversion options
                         * @returns {Object.<string,*>} Plain object
                         */
                        public static toObject(message: cockroach.ccl.storageccl.engineccl.enginepbccl.EncryptionStatus, options?: $protobuf.ConversionOptions): { [k: string]: any };

                        /**
                         * Creates a plain object from this EncryptionStatus message. Also converts values to other types if specified.
                         * @param {$protobuf.ConversionOptions} [options] Conversion options
                         * @returns {Object.<string,*>} Plain object
                         */
                        public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

                        /**
                         * Converts this EncryptionStatus to JSON.
                         * @returns {Object.<string,*>} JSON object
                         */
                        public toJSON(): { [k: string]: any };
                    }
                }
            }
        }
    }
}

/**
 * Namespace gogoproto.
 * @exports gogoproto
 * @namespace
 */
export namespace gogoproto {
}

/**
 * Namespace google.
 * @exports google
 * @namespace
 */
export namespace google {

    /**
     * Namespace protobuf.
     * @exports google.protobuf
     * @namespace
     */
    namespace protobuf {

        type FileDescriptorSet$Properties = {
            file?: google.protobuf.FileDescriptorProto$Properties[];
        };

        /**
         * Constructs a new FileDescriptorSet.
         * @exports google.protobuf.FileDescriptorSet
         * @constructor
         * @param {google.protobuf.FileDescriptorSet$Properties=} [properties] Properties to set
         */
        class FileDescriptorSet {

            /**
             * Constructs a new FileDescriptorSet.
             * @exports google.protobuf.FileDescriptorSet
             * @constructor
             * @param {google.protobuf.FileDescriptorSet$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.FileDescriptorSet$Properties);

            /**
             * FileDescriptorSet file.
             * @type {Array.<google.protobuf.FileDescriptorProto$Properties>}
             */
            public file: google.protobuf.FileDescriptorProto$Properties[];

            /**
             * Creates a new FileDescriptorSet instance using the specified properties.
             * @param {google.protobuf.FileDescriptorSet$Properties=} [properties] Properties to set
             * @returns {google.protobuf.FileDescriptorSet} FileDescriptorSet instance
             */
            public static create(properties?: google.protobuf.FileDescriptorSet$Properties): google.protobuf.FileDescriptorSet;

            /**
             * Encodes the specified FileDescriptorSet message. Does not implicitly {@link google.protobuf.FileDescriptorSet.verify|verify} messages.
             * @param {google.protobuf.FileDescriptorSet$Properties} message FileDescriptorSet message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.FileDescriptorSet$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified FileDescriptorSet message, length delimited. Does not implicitly {@link google.protobuf.FileDescriptorSet.verify|verify} messages.
             * @param {google.protobuf.FileDescriptorSet$Properties} message FileDescriptorSet message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.FileDescriptorSet$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a FileDescriptorSet message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.FileDescriptorSet} FileDescriptorSet
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.FileDescriptorSet;

            /**
             * Decodes a FileDescriptorSet message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.FileDescriptorSet} FileDescriptorSet
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.FileDescriptorSet;

            /**
             * Verifies a FileDescriptorSet message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a FileDescriptorSet message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.FileDescriptorSet} FileDescriptorSet
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.FileDescriptorSet;

            /**
             * Creates a FileDescriptorSet message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.FileDescriptorSet.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.FileDescriptorSet} FileDescriptorSet
             */
            public static from(object: { [k: string]: any }): google.protobuf.FileDescriptorSet;

            /**
             * Creates a plain object from a FileDescriptorSet message. Also converts values to other types if specified.
             * @param {google.protobuf.FileDescriptorSet} message FileDescriptorSet
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.FileDescriptorSet, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this FileDescriptorSet message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this FileDescriptorSet to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type FileDescriptorProto$Properties = {
            name?: string;
            "package"?: string;
            dependency?: string[];
            public_dependency?: number[];
            weak_dependency?: number[];
            message_type?: google.protobuf.DescriptorProto$Properties[];
            enum_type?: google.protobuf.EnumDescriptorProto$Properties[];
            service?: google.protobuf.ServiceDescriptorProto$Properties[];
            extension?: google.protobuf.FieldDescriptorProto$Properties[];
            options?: google.protobuf.FileOptions$Properties;
            source_code_info?: google.protobuf.SourceCodeInfo$Properties;
            syntax?: string;
        };

        /**
         * Constructs a new FileDescriptorProto.
         * @exports google.protobuf.FileDescriptorProto
         * @constructor
         * @param {google.protobuf.FileDescriptorProto$Properties=} [properties] Properties to set
         */
        class FileDescriptorProto {

            /**
             * Constructs a new FileDescriptorProto.
             * @exports google.protobuf.FileDescriptorProto
             * @constructor
             * @param {google.protobuf.FileDescriptorProto$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.FileDescriptorProto$Properties);

            /**
             * FileDescriptorProto name.
             * @type {string}
             */
            public name: string;

            /**
             * FileDescriptorProto package.
             * @type {string}
             */
            public ["package"]: string;

            /**
             * FileDescriptorProto dependency.
             * @type {Array.<string>}
             */
            public dependency: string[];

            /**
             * FileDescriptorProto public_dependency.
             * @type {Array.<number>}
             */
            public public_dependency: number[];

            /**
             * FileDescriptorProto weak_dependency.
             * @type {Array.<number>}
             */
            public weak_dependency: number[];

            /**
             * FileDescriptorProto message_type.
             * @type {Array.<google.protobuf.DescriptorProto$Properties>}
             */
            public message_type: google.protobuf.DescriptorProto$Properties[];

            /**
             * FileDescriptorProto enum_type.
             * @type {Array.<google.protobuf.EnumDescriptorProto$Properties>}
             */
            public enum_type: google.protobuf.EnumDescriptorProto$Properties[];

            /**
             * FileDescriptorProto service.
             * @type {Array.<google.protobuf.ServiceDescriptorProto$Properties>}
             */
            public service: google.protobuf.ServiceDescriptorProto$Properties[];

            /**
             * FileDescriptorProto extension.
             * @type {Array.<google.protobuf.FieldDescriptorProto$Properties>}
             */
            public extension: google.protobuf.FieldDescriptorProto$Properties[];

            /**
             * FileDescriptorProto options.
             * @type {(google.protobuf.FileOptions$Properties|null)}
             */
            public options: (google.protobuf.FileOptions$Properties|null);

            /**
             * FileDescriptorProto source_code_info.
             * @type {(google.protobuf.SourceCodeInfo$Properties|null)}
             */
            public source_code_info: (google.protobuf.SourceCodeInfo$Properties|null);

            /**
             * FileDescriptorProto syntax.
             * @type {string}
             */
            public syntax: string;

            /**
             * Creates a new FileDescriptorProto instance using the specified properties.
             * @param {google.protobuf.FileDescriptorProto$Properties=} [properties] Properties to set
             * @returns {google.protobuf.FileDescriptorProto} FileDescriptorProto instance
             */
            public static create(properties?: google.protobuf.FileDescriptorProto$Properties): google.protobuf.FileDescriptorProto;

            /**
             * Encodes the specified FileDescriptorProto message. Does not implicitly {@link google.protobuf.FileDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.FileDescriptorProto$Properties} message FileDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.FileDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified FileDescriptorProto message, length delimited. Does not implicitly {@link google.protobuf.FileDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.FileDescriptorProto$Properties} message FileDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.FileDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a FileDescriptorProto message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.FileDescriptorProto} FileDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.FileDescriptorProto;

            /**
             * Decodes a FileDescriptorProto message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.FileDescriptorProto} FileDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.FileDescriptorProto;

            /**
             * Verifies a FileDescriptorProto message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a FileDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.FileDescriptorProto} FileDescriptorProto
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.FileDescriptorProto;

            /**
             * Creates a FileDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.FileDescriptorProto.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.FileDescriptorProto} FileDescriptorProto
             */
            public static from(object: { [k: string]: any }): google.protobuf.FileDescriptorProto;

            /**
             * Creates a plain object from a FileDescriptorProto message. Also converts values to other types if specified.
             * @param {google.protobuf.FileDescriptorProto} message FileDescriptorProto
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.FileDescriptorProto, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this FileDescriptorProto message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this FileDescriptorProto to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type DescriptorProto$Properties = {
            name?: string;
            field?: google.protobuf.FieldDescriptorProto$Properties[];
            extension?: google.protobuf.FieldDescriptorProto$Properties[];
            nested_type?: google.protobuf.DescriptorProto$Properties[];
            enum_type?: google.protobuf.EnumDescriptorProto$Properties[];
            extension_range?: google.protobuf.DescriptorProto.ExtensionRange$Properties[];
            oneof_decl?: google.protobuf.OneofDescriptorProto$Properties[];
            options?: google.protobuf.MessageOptions$Properties;
            reserved_range?: google.protobuf.DescriptorProto.ReservedRange$Properties[];
            reserved_name?: string[];
        };

        /**
         * Constructs a new DescriptorProto.
         * @exports google.protobuf.DescriptorProto
         * @constructor
         * @param {google.protobuf.DescriptorProto$Properties=} [properties] Properties to set
         */
        class DescriptorProto {

            /**
             * Constructs a new DescriptorProto.
             * @exports google.protobuf.DescriptorProto
             * @constructor
             * @param {google.protobuf.DescriptorProto$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.DescriptorProto$Properties);

            /**
             * DescriptorProto name.
             * @type {string}
             */
            public name: string;

            /**
             * DescriptorProto field.
             * @type {Array.<google.protobuf.FieldDescriptorProto$Properties>}
             */
            public field: google.protobuf.FieldDescriptorProto$Properties[];

            /**
             * DescriptorProto extension.
             * @type {Array.<google.protobuf.FieldDescriptorProto$Properties>}
             */
            public extension: google.protobuf.FieldDescriptorProto$Properties[];

            /**
             * DescriptorProto nested_type.
             * @type {Array.<google.protobuf.DescriptorProto$Properties>}
             */
            public nested_type: google.protobuf.DescriptorProto$Properties[];

            /**
             * DescriptorProto enum_type.
             * @type {Array.<google.protobuf.EnumDescriptorProto$Properties>}
             */
            public enum_type: google.protobuf.EnumDescriptorProto$Properties[];

            /**
             * DescriptorProto extension_range.
             * @type {Array.<google.protobuf.DescriptorProto.ExtensionRange$Properties>}
             */
            public extension_range: google.protobuf.DescriptorProto.ExtensionRange$Properties[];

            /**
             * DescriptorProto oneof_decl.
             * @type {Array.<google.protobuf.OneofDescriptorProto$Properties>}
             */
            public oneof_decl: google.protobuf.OneofDescriptorProto$Properties[];

            /**
             * DescriptorProto options.
             * @type {(google.protobuf.MessageOptions$Properties|null)}
             */
            public options: (google.protobuf.MessageOptions$Properties|null);

            /**
             * DescriptorProto reserved_range.
             * @type {Array.<google.protobuf.DescriptorProto.ReservedRange$Properties>}
             */
            public reserved_range: google.protobuf.DescriptorProto.ReservedRange$Properties[];

            /**
             * DescriptorProto reserved_name.
             * @type {Array.<string>}
             */
            public reserved_name: string[];

            /**
             * Creates a new DescriptorProto instance using the specified properties.
             * @param {google.protobuf.DescriptorProto$Properties=} [properties] Properties to set
             * @returns {google.protobuf.DescriptorProto} DescriptorProto instance
             */
            public static create(properties?: google.protobuf.DescriptorProto$Properties): google.protobuf.DescriptorProto;

            /**
             * Encodes the specified DescriptorProto message. Does not implicitly {@link google.protobuf.DescriptorProto.verify|verify} messages.
             * @param {google.protobuf.DescriptorProto$Properties} message DescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.DescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified DescriptorProto message, length delimited. Does not implicitly {@link google.protobuf.DescriptorProto.verify|verify} messages.
             * @param {google.protobuf.DescriptorProto$Properties} message DescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.DescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a DescriptorProto message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.DescriptorProto} DescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.DescriptorProto;

            /**
             * Decodes a DescriptorProto message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.DescriptorProto} DescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.DescriptorProto;

            /**
             * Verifies a DescriptorProto message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a DescriptorProto message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.DescriptorProto} DescriptorProto
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.DescriptorProto;

            /**
             * Creates a DescriptorProto message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.DescriptorProto.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.DescriptorProto} DescriptorProto
             */
            public static from(object: { [k: string]: any }): google.protobuf.DescriptorProto;

            /**
             * Creates a plain object from a DescriptorProto message. Also converts values to other types if specified.
             * @param {google.protobuf.DescriptorProto} message DescriptorProto
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.DescriptorProto, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this DescriptorProto message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this DescriptorProto to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        namespace DescriptorProto {

            type ExtensionRange$Properties = {
                start?: number;
                end?: number;
            };

            /**
             * Constructs a new ExtensionRange.
             * @exports google.protobuf.DescriptorProto.ExtensionRange
             * @constructor
             * @param {google.protobuf.DescriptorProto.ExtensionRange$Properties=} [properties] Properties to set
             */
            class ExtensionRange {

                /**
                 * Constructs a new ExtensionRange.
                 * @exports google.protobuf.DescriptorProto.ExtensionRange
                 * @constructor
                 * @param {google.protobuf.DescriptorProto.ExtensionRange$Properties=} [properties] Properties to set
                 */
                constructor(properties?: google.protobuf.DescriptorProto.ExtensionRange$Properties);

                /**
                 * ExtensionRange start.
                 * @type {number}
                 */
                public start: number;

                /**
                 * ExtensionRange end.
                 * @type {number}
                 */
                public end: number;

                /**
                 * Creates a new ExtensionRange instance using the specified properties.
                 * @param {google.protobuf.DescriptorProto.ExtensionRange$Properties=} [properties] Properties to set
                 * @returns {google.protobuf.DescriptorProto.ExtensionRange} ExtensionRange instance
                 */
                public static create(properties?: google.protobuf.DescriptorProto.ExtensionRange$Properties): google.protobuf.DescriptorProto.ExtensionRange;

                /**
                 * Encodes the specified ExtensionRange message. Does not implicitly {@link google.protobuf.DescriptorProto.ExtensionRange.verify|verify} messages.
                 * @param {google.protobuf.DescriptorProto.ExtensionRange$Properties} message ExtensionRange message or plain object to encode
                 * @param {$protobuf.Writer} [writer] Writer to encode to
                 * @returns {$protobuf.Writer} Writer
                 */
                public static encode(message: google.protobuf.DescriptorProto.ExtensionRange$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                /**
                 * Encodes the specified ExtensionRange message, length delimited. Does not implicitly {@link google.protobuf.DescriptorProto.ExtensionRange.verify|verify} messages.
                 * @param {google.protobuf.DescriptorProto.ExtensionRange$Properties} message ExtensionRange message or plain object to encode
                 * @param {$protobuf.Writer} [writer] Writer to encode to
                 * @returns {$protobuf.Writer} Writer
                 */
                public static encodeDelimited(message: google.protobuf.DescriptorProto.ExtensionRange$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                /**
                 * Decodes an ExtensionRange message from the specified reader or buffer.
                 * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                 * @param {number} [length] Message length if known beforehand
                 * @returns {google.protobuf.DescriptorProto.ExtensionRange} ExtensionRange
                 * @throws {Error} If the payload is not a reader or valid buffer
                 * @throws {$protobuf.util.ProtocolError} If required fields are missing
                 */
                public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.DescriptorProto.ExtensionRange;

                /**
                 * Decodes an ExtensionRange message from the specified reader or buffer, length delimited.
                 * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                 * @returns {google.protobuf.DescriptorProto.ExtensionRange} ExtensionRange
                 * @throws {Error} If the payload is not a reader or valid buffer
                 * @throws {$protobuf.util.ProtocolError} If required fields are missing
                 */
                public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.DescriptorProto.ExtensionRange;

                /**
                 * Verifies an ExtensionRange message.
                 * @param {Object.<string,*>} message Plain object to verify
                 * @returns {?string} `null` if valid, otherwise the reason why it is not
                 */
                public static verify(message: { [k: string]: any }): string;

                /**
                 * Creates an ExtensionRange message from a plain object. Also converts values to their respective internal types.
                 * @param {Object.<string,*>} object Plain object
                 * @returns {google.protobuf.DescriptorProto.ExtensionRange} ExtensionRange
                 */
                public static fromObject(object: { [k: string]: any }): google.protobuf.DescriptorProto.ExtensionRange;

                /**
                 * Creates an ExtensionRange message from a plain object. Also converts values to their respective internal types.
                 * This is an alias of {@link google.protobuf.DescriptorProto.ExtensionRange.fromObject}.
                 * @function
                 * @param {Object.<string,*>} object Plain object
                 * @returns {google.protobuf.DescriptorProto.ExtensionRange} ExtensionRange
                 */
                public static from(object: { [k: string]: any }): google.protobuf.DescriptorProto.ExtensionRange;

                /**
                 * Creates a plain object from an ExtensionRange message. Also converts values to other types if specified.
                 * @param {google.protobuf.DescriptorProto.ExtensionRange} message ExtensionRange
                 * @param {$protobuf.ConversionOptions} [options] Conversion options
                 * @returns {Object.<string,*>} Plain object
                 */
                public static toObject(message: google.protobuf.DescriptorProto.ExtensionRange, options?: $protobuf.ConversionOptions): { [k: string]: any };

                /**
                 * Creates a plain object from this ExtensionRange message. Also converts values to other types if specified.
                 * @param {$protobuf.ConversionOptions} [options] Conversion options
                 * @returns {Object.<string,*>} Plain object
                 */
                public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

                /**
                 * Converts this ExtensionRange to JSON.
                 * @returns {Object.<string,*>} JSON object
                 */
                public toJSON(): { [k: string]: any };
            }

            type ReservedRange$Properties = {
                start?: number;
                end?: number;
            };

            /**
             * Constructs a new ReservedRange.
             * @exports google.protobuf.DescriptorProto.ReservedRange
             * @constructor
             * @param {google.protobuf.DescriptorProto.ReservedRange$Properties=} [properties] Properties to set
             */
            class ReservedRange {

                /**
                 * Constructs a new ReservedRange.
                 * @exports google.protobuf.DescriptorProto.ReservedRange
                 * @constructor
                 * @param {google.protobuf.DescriptorProto.ReservedRange$Properties=} [properties] Properties to set
                 */
                constructor(properties?: google.protobuf.DescriptorProto.ReservedRange$Properties);

                /**
                 * ReservedRange start.
                 * @type {number}
                 */
                public start: number;

                /**
                 * ReservedRange end.
                 * @type {number}
                 */
                public end: number;

                /**
                 * Creates a new ReservedRange instance using the specified properties.
                 * @param {google.protobuf.DescriptorProto.ReservedRange$Properties=} [properties] Properties to set
                 * @returns {google.protobuf.DescriptorProto.ReservedRange} ReservedRange instance
                 */
                public static create(properties?: google.protobuf.DescriptorProto.ReservedRange$Properties): google.protobuf.DescriptorProto.ReservedRange;

                /**
                 * Encodes the specified ReservedRange message. Does not implicitly {@link google.protobuf.DescriptorProto.ReservedRange.verify|verify} messages.
                 * @param {google.protobuf.DescriptorProto.ReservedRange$Properties} message ReservedRange message or plain object to encode
                 * @param {$protobuf.Writer} [writer] Writer to encode to
                 * @returns {$protobuf.Writer} Writer
                 */
                public static encode(message: google.protobuf.DescriptorProto.ReservedRange$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                /**
                 * Encodes the specified ReservedRange message, length delimited. Does not implicitly {@link google.protobuf.DescriptorProto.ReservedRange.verify|verify} messages.
                 * @param {google.protobuf.DescriptorProto.ReservedRange$Properties} message ReservedRange message or plain object to encode
                 * @param {$protobuf.Writer} [writer] Writer to encode to
                 * @returns {$protobuf.Writer} Writer
                 */
                public static encodeDelimited(message: google.protobuf.DescriptorProto.ReservedRange$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                /**
                 * Decodes a ReservedRange message from the specified reader or buffer.
                 * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                 * @param {number} [length] Message length if known beforehand
                 * @returns {google.protobuf.DescriptorProto.ReservedRange} ReservedRange
                 * @throws {Error} If the payload is not a reader or valid buffer
                 * @throws {$protobuf.util.ProtocolError} If required fields are missing
                 */
                public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.DescriptorProto.ReservedRange;

                /**
                 * Decodes a ReservedRange message from the specified reader or buffer, length delimited.
                 * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                 * @returns {google.protobuf.DescriptorProto.ReservedRange} ReservedRange
                 * @throws {Error} If the payload is not a reader or valid buffer
                 * @throws {$protobuf.util.ProtocolError} If required fields are missing
                 */
                public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.DescriptorProto.ReservedRange;

                /**
                 * Verifies a ReservedRange message.
                 * @param {Object.<string,*>} message Plain object to verify
                 * @returns {?string} `null` if valid, otherwise the reason why it is not
                 */
                public static verify(message: { [k: string]: any }): string;

                /**
                 * Creates a ReservedRange message from a plain object. Also converts values to their respective internal types.
                 * @param {Object.<string,*>} object Plain object
                 * @returns {google.protobuf.DescriptorProto.ReservedRange} ReservedRange
                 */
                public static fromObject(object: { [k: string]: any }): google.protobuf.DescriptorProto.ReservedRange;

                /**
                 * Creates a ReservedRange message from a plain object. Also converts values to their respective internal types.
                 * This is an alias of {@link google.protobuf.DescriptorProto.ReservedRange.fromObject}.
                 * @function
                 * @param {Object.<string,*>} object Plain object
                 * @returns {google.protobuf.DescriptorProto.ReservedRange} ReservedRange
                 */
                public static from(object: { [k: string]: any }): google.protobuf.DescriptorProto.ReservedRange;

                /**
                 * Creates a plain object from a ReservedRange message. Also converts values to other types if specified.
                 * @param {google.protobuf.DescriptorProto.ReservedRange} message ReservedRange
                 * @param {$protobuf.ConversionOptions} [options] Conversion options
                 * @returns {Object.<string,*>} Plain object
                 */
                public static toObject(message: google.protobuf.DescriptorProto.ReservedRange, options?: $protobuf.ConversionOptions): { [k: string]: any };

                /**
                 * Creates a plain object from this ReservedRange message. Also converts values to other types if specified.
                 * @param {$protobuf.ConversionOptions} [options] Conversion options
                 * @returns {Object.<string,*>} Plain object
                 */
                public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

                /**
                 * Converts this ReservedRange to JSON.
                 * @returns {Object.<string,*>} JSON object
                 */
                public toJSON(): { [k: string]: any };
            }
        }

        type FieldDescriptorProto$Properties = {
            name?: string;
            number?: number;
            label?: google.protobuf.FieldDescriptorProto.Label;
            type?: google.protobuf.FieldDescriptorProto.Type;
            type_name?: string;
            extendee?: string;
            default_value?: string;
            oneof_index?: number;
            json_name?: string;
            options?: google.protobuf.FieldOptions$Properties;
        };

        /**
         * Constructs a new FieldDescriptorProto.
         * @exports google.protobuf.FieldDescriptorProto
         * @constructor
         * @param {google.protobuf.FieldDescriptorProto$Properties=} [properties] Properties to set
         */
        class FieldDescriptorProto {

            /**
             * Constructs a new FieldDescriptorProto.
             * @exports google.protobuf.FieldDescriptorProto
             * @constructor
             * @param {google.protobuf.FieldDescriptorProto$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.FieldDescriptorProto$Properties);

            /**
             * FieldDescriptorProto name.
             * @type {string}
             */
            public name: string;

            /**
             * FieldDescriptorProto number.
             * @type {number}
             */
            public number: number;

            /**
             * FieldDescriptorProto label.
             * @type {google.protobuf.FieldDescriptorProto.Label}
             */
            public label: google.protobuf.FieldDescriptorProto.Label;

            /**
             * FieldDescriptorProto type.
             * @type {google.protobuf.FieldDescriptorProto.Type}
             */
            public type: google.protobuf.FieldDescriptorProto.Type;

            /**
             * FieldDescriptorProto type_name.
             * @type {string}
             */
            public type_name: string;

            /**
             * FieldDescriptorProto extendee.
             * @type {string}
             */
            public extendee: string;

            /**
             * FieldDescriptorProto default_value.
             * @type {string}
             */
            public default_value: string;

            /**
             * FieldDescriptorProto oneof_index.
             * @type {number}
             */
            public oneof_index: number;

            /**
             * FieldDescriptorProto json_name.
             * @type {string}
             */
            public json_name: string;

            /**
             * FieldDescriptorProto options.
             * @type {(google.protobuf.FieldOptions$Properties|null)}
             */
            public options: (google.protobuf.FieldOptions$Properties|null);

            /**
             * Creates a new FieldDescriptorProto instance using the specified properties.
             * @param {google.protobuf.FieldDescriptorProto$Properties=} [properties] Properties to set
             * @returns {google.protobuf.FieldDescriptorProto} FieldDescriptorProto instance
             */
            public static create(properties?: google.protobuf.FieldDescriptorProto$Properties): google.protobuf.FieldDescriptorProto;

            /**
             * Encodes the specified FieldDescriptorProto message. Does not implicitly {@link google.protobuf.FieldDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.FieldDescriptorProto$Properties} message FieldDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.FieldDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified FieldDescriptorProto message, length delimited. Does not implicitly {@link google.protobuf.FieldDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.FieldDescriptorProto$Properties} message FieldDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.FieldDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a FieldDescriptorProto message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.FieldDescriptorProto} FieldDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.FieldDescriptorProto;

            /**
             * Decodes a FieldDescriptorProto message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.FieldDescriptorProto} FieldDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.FieldDescriptorProto;

            /**
             * Verifies a FieldDescriptorProto message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a FieldDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.FieldDescriptorProto} FieldDescriptorProto
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.FieldDescriptorProto;

            /**
             * Creates a FieldDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.FieldDescriptorProto.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.FieldDescriptorProto} FieldDescriptorProto
             */
            public static from(object: { [k: string]: any }): google.protobuf.FieldDescriptorProto;

            /**
             * Creates a plain object from a FieldDescriptorProto message. Also converts values to other types if specified.
             * @param {google.protobuf.FieldDescriptorProto} message FieldDescriptorProto
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.FieldDescriptorProto, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this FieldDescriptorProto message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this FieldDescriptorProto to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        namespace FieldDescriptorProto {

            /**
             * Type enum.
             * @name Type
             * @memberof google.protobuf.FieldDescriptorProto
             * @enum {number}
             * @property {number} TYPE_DOUBLE=1 TYPE_DOUBLE value
             * @property {number} TYPE_FLOAT=2 TYPE_FLOAT value
             * @property {number} TYPE_INT64=3 TYPE_INT64 value
             * @property {number} TYPE_UINT64=4 TYPE_UINT64 value
             * @property {number} TYPE_INT32=5 TYPE_INT32 value
             * @property {number} TYPE_FIXED64=6 TYPE_FIXED64 value
             * @property {number} TYPE_FIXED32=7 TYPE_FIXED32 value
             * @property {number} TYPE_BOOL=8 TYPE_BOOL value
             * @property {number} TYPE_STRING=9 TYPE_STRING value
             * @property {number} TYPE_GROUP=10 TYPE_GROUP value
             * @property {number} TYPE_MESSAGE=11 TYPE_MESSAGE value
             * @property {number} TYPE_BYTES=12 TYPE_BYTES value
             * @property {number} TYPE_UINT32=13 TYPE_UINT32 value
             * @property {number} TYPE_ENUM=14 TYPE_ENUM value
             * @property {number} TYPE_SFIXED32=15 TYPE_SFIXED32 value
             * @property {number} TYPE_SFIXED64=16 TYPE_SFIXED64 value
             * @property {number} TYPE_SINT32=17 TYPE_SINT32 value
             * @property {number} TYPE_SINT64=18 TYPE_SINT64 value
             */
            enum Type {
                TYPE_DOUBLE = 1,
                TYPE_FLOAT = 2,
                TYPE_INT64 = 3,
                TYPE_UINT64 = 4,
                TYPE_INT32 = 5,
                TYPE_FIXED64 = 6,
                TYPE_FIXED32 = 7,
                TYPE_BOOL = 8,
                TYPE_STRING = 9,
                TYPE_GROUP = 10,
                TYPE_MESSAGE = 11,
                TYPE_BYTES = 12,
                TYPE_UINT32 = 13,
                TYPE_ENUM = 14,
                TYPE_SFIXED32 = 15,
                TYPE_SFIXED64 = 16,
                TYPE_SINT32 = 17,
                TYPE_SINT64 = 18
            }

            /**
             * Label enum.
             * @name Label
             * @memberof google.protobuf.FieldDescriptorProto
             * @enum {number}
             * @property {number} LABEL_OPTIONAL=1 LABEL_OPTIONAL value
             * @property {number} LABEL_REQUIRED=2 LABEL_REQUIRED value
             * @property {number} LABEL_REPEATED=3 LABEL_REPEATED value
             */
            enum Label {
                LABEL_OPTIONAL = 1,
                LABEL_REQUIRED = 2,
                LABEL_REPEATED = 3
            }
        }

        type OneofDescriptorProto$Properties = {
            name?: string;
            options?: google.protobuf.OneofOptions$Properties;
        };

        /**
         * Constructs a new OneofDescriptorProto.
         * @exports google.protobuf.OneofDescriptorProto
         * @constructor
         * @param {google.protobuf.OneofDescriptorProto$Properties=} [properties] Properties to set
         */
        class OneofDescriptorProto {

            /**
             * Constructs a new OneofDescriptorProto.
             * @exports google.protobuf.OneofDescriptorProto
             * @constructor
             * @param {google.protobuf.OneofDescriptorProto$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.OneofDescriptorProto$Properties);

            /**
             * OneofDescriptorProto name.
             * @type {string}
             */
            public name: string;

            /**
             * OneofDescriptorProto options.
             * @type {(google.protobuf.OneofOptions$Properties|null)}
             */
            public options: (google.protobuf.OneofOptions$Properties|null);

            /**
             * Creates a new OneofDescriptorProto instance using the specified properties.
             * @param {google.protobuf.OneofDescriptorProto$Properties=} [properties] Properties to set
             * @returns {google.protobuf.OneofDescriptorProto} OneofDescriptorProto instance
             */
            public static create(properties?: google.protobuf.OneofDescriptorProto$Properties): google.protobuf.OneofDescriptorProto;

            /**
             * Encodes the specified OneofDescriptorProto message. Does not implicitly {@link google.protobuf.OneofDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.OneofDescriptorProto$Properties} message OneofDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.OneofDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified OneofDescriptorProto message, length delimited. Does not implicitly {@link google.protobuf.OneofDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.OneofDescriptorProto$Properties} message OneofDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.OneofDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes an OneofDescriptorProto message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.OneofDescriptorProto} OneofDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.OneofDescriptorProto;

            /**
             * Decodes an OneofDescriptorProto message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.OneofDescriptorProto} OneofDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.OneofDescriptorProto;

            /**
             * Verifies an OneofDescriptorProto message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates an OneofDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.OneofDescriptorProto} OneofDescriptorProto
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.OneofDescriptorProto;

            /**
             * Creates an OneofDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.OneofDescriptorProto.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.OneofDescriptorProto} OneofDescriptorProto
             */
            public static from(object: { [k: string]: any }): google.protobuf.OneofDescriptorProto;

            /**
             * Creates a plain object from an OneofDescriptorProto message. Also converts values to other types if specified.
             * @param {google.protobuf.OneofDescriptorProto} message OneofDescriptorProto
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.OneofDescriptorProto, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this OneofDescriptorProto message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this OneofDescriptorProto to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type EnumDescriptorProto$Properties = {
            name?: string;
            value?: google.protobuf.EnumValueDescriptorProto$Properties[];
            options?: google.protobuf.EnumOptions$Properties;
        };

        /**
         * Constructs a new EnumDescriptorProto.
         * @exports google.protobuf.EnumDescriptorProto
         * @constructor
         * @param {google.protobuf.EnumDescriptorProto$Properties=} [properties] Properties to set
         */
        class EnumDescriptorProto {

            /**
             * Constructs a new EnumDescriptorProto.
             * @exports google.protobuf.EnumDescriptorProto
             * @constructor
             * @param {google.protobuf.EnumDescriptorProto$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.EnumDescriptorProto$Properties);

            /**
             * EnumDescriptorProto name.
             * @type {string}
             */
            public name: string;

            /**
             * EnumDescriptorProto value.
             * @type {Array.<google.protobuf.EnumValueDescriptorProto$Properties>}
             */
            public value: google.protobuf.EnumValueDescriptorProto$Properties[];

            /**
             * EnumDescriptorProto options.
             * @type {(google.protobuf.EnumOptions$Properties|null)}
             */
            public options: (google.protobuf.EnumOptions$Properties|null);

            /**
             * Creates a new EnumDescriptorProto instance using the specified properties.
             * @param {google.protobuf.EnumDescriptorProto$Properties=} [properties] Properties to set
             * @returns {google.protobuf.EnumDescriptorProto} EnumDescriptorProto instance
             */
            public static create(properties?: google.protobuf.EnumDescriptorProto$Properties): google.protobuf.EnumDescriptorProto;

            /**
             * Encodes the specified EnumDescriptorProto message. Does not implicitly {@link google.protobuf.EnumDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.EnumDescriptorProto$Properties} message EnumDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.EnumDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified EnumDescriptorProto message, length delimited. Does not implicitly {@link google.protobuf.EnumDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.EnumDescriptorProto$Properties} message EnumDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.EnumDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes an EnumDescriptorProto message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.EnumDescriptorProto} EnumDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.EnumDescriptorProto;

            /**
             * Decodes an EnumDescriptorProto message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.EnumDescriptorProto} EnumDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.EnumDescriptorProto;

            /**
             * Verifies an EnumDescriptorProto message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates an EnumDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.EnumDescriptorProto} EnumDescriptorProto
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.EnumDescriptorProto;

            /**
             * Creates an EnumDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.EnumDescriptorProto.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.EnumDescriptorProto} EnumDescriptorProto
             */
            public static from(object: { [k: string]: any }): google.protobuf.EnumDescriptorProto;

            /**
             * Creates a plain object from an EnumDescriptorProto message. Also converts values to other types if specified.
             * @param {google.protobuf.EnumDescriptorProto} message EnumDescriptorProto
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.EnumDescriptorProto, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this EnumDescriptorProto message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this EnumDescriptorProto to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type EnumValueDescriptorProto$Properties = {
            name?: string;
            number?: number;
            options?: google.protobuf.EnumValueOptions$Properties;
        };

        /**
         * Constructs a new EnumValueDescriptorProto.
         * @exports google.protobuf.EnumValueDescriptorProto
         * @constructor
         * @param {google.protobuf.EnumValueDescriptorProto$Properties=} [properties] Properties to set
         */
        class EnumValueDescriptorProto {

            /**
             * Constructs a new EnumValueDescriptorProto.
             * @exports google.protobuf.EnumValueDescriptorProto
             * @constructor
             * @param {google.protobuf.EnumValueDescriptorProto$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.EnumValueDescriptorProto$Properties);

            /**
             * EnumValueDescriptorProto name.
             * @type {string}
             */
            public name: string;

            /**
             * EnumValueDescriptorProto number.
             * @type {number}
             */
            public number: number;

            /**
             * EnumValueDescriptorProto options.
             * @type {(google.protobuf.EnumValueOptions$Properties|null)}
             */
            public options: (google.protobuf.EnumValueOptions$Properties|null);

            /**
             * Creates a new EnumValueDescriptorProto instance using the specified properties.
             * @param {google.protobuf.EnumValueDescriptorProto$Properties=} [properties] Properties to set
             * @returns {google.protobuf.EnumValueDescriptorProto} EnumValueDescriptorProto instance
             */
            public static create(properties?: google.protobuf.EnumValueDescriptorProto$Properties): google.protobuf.EnumValueDescriptorProto;

            /**
             * Encodes the specified EnumValueDescriptorProto message. Does not implicitly {@link google.protobuf.EnumValueDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.EnumValueDescriptorProto$Properties} message EnumValueDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.EnumValueDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified EnumValueDescriptorProto message, length delimited. Does not implicitly {@link google.protobuf.EnumValueDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.EnumValueDescriptorProto$Properties} message EnumValueDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.EnumValueDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes an EnumValueDescriptorProto message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.EnumValueDescriptorProto} EnumValueDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.EnumValueDescriptorProto;

            /**
             * Decodes an EnumValueDescriptorProto message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.EnumValueDescriptorProto} EnumValueDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.EnumValueDescriptorProto;

            /**
             * Verifies an EnumValueDescriptorProto message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates an EnumValueDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.EnumValueDescriptorProto} EnumValueDescriptorProto
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.EnumValueDescriptorProto;

            /**
             * Creates an EnumValueDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.EnumValueDescriptorProto.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.EnumValueDescriptorProto} EnumValueDescriptorProto
             */
            public static from(object: { [k: string]: any }): google.protobuf.EnumValueDescriptorProto;

            /**
             * Creates a plain object from an EnumValueDescriptorProto message. Also converts values to other types if specified.
             * @param {google.protobuf.EnumValueDescriptorProto} message EnumValueDescriptorProto
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.EnumValueDescriptorProto, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this EnumValueDescriptorProto message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this EnumValueDescriptorProto to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type ServiceDescriptorProto$Properties = {
            name?: string;
            method?: google.protobuf.MethodDescriptorProto$Properties[];
            options?: google.protobuf.ServiceOptions$Properties;
        };

        /**
         * Constructs a new ServiceDescriptorProto.
         * @exports google.protobuf.ServiceDescriptorProto
         * @constructor
         * @param {google.protobuf.ServiceDescriptorProto$Properties=} [properties] Properties to set
         */
        class ServiceDescriptorProto {

            /**
             * Constructs a new ServiceDescriptorProto.
             * @exports google.protobuf.ServiceDescriptorProto
             * @constructor
             * @param {google.protobuf.ServiceDescriptorProto$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.ServiceDescriptorProto$Properties);

            /**
             * ServiceDescriptorProto name.
             * @type {string}
             */
            public name: string;

            /**
             * ServiceDescriptorProto method.
             * @type {Array.<google.protobuf.MethodDescriptorProto$Properties>}
             */
            public method: google.protobuf.MethodDescriptorProto$Properties[];

            /**
             * ServiceDescriptorProto options.
             * @type {(google.protobuf.ServiceOptions$Properties|null)}
             */
            public options: (google.protobuf.ServiceOptions$Properties|null);

            /**
             * Creates a new ServiceDescriptorProto instance using the specified properties.
             * @param {google.protobuf.ServiceDescriptorProto$Properties=} [properties] Properties to set
             * @returns {google.protobuf.ServiceDescriptorProto} ServiceDescriptorProto instance
             */
            public static create(properties?: google.protobuf.ServiceDescriptorProto$Properties): google.protobuf.ServiceDescriptorProto;

            /**
             * Encodes the specified ServiceDescriptorProto message. Does not implicitly {@link google.protobuf.ServiceDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.ServiceDescriptorProto$Properties} message ServiceDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.ServiceDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified ServiceDescriptorProto message, length delimited. Does not implicitly {@link google.protobuf.ServiceDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.ServiceDescriptorProto$Properties} message ServiceDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.ServiceDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a ServiceDescriptorProto message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.ServiceDescriptorProto} ServiceDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.ServiceDescriptorProto;

            /**
             * Decodes a ServiceDescriptorProto message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.ServiceDescriptorProto} ServiceDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.ServiceDescriptorProto;

            /**
             * Verifies a ServiceDescriptorProto message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a ServiceDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.ServiceDescriptorProto} ServiceDescriptorProto
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.ServiceDescriptorProto;

            /**
             * Creates a ServiceDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.ServiceDescriptorProto.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.ServiceDescriptorProto} ServiceDescriptorProto
             */
            public static from(object: { [k: string]: any }): google.protobuf.ServiceDescriptorProto;

            /**
             * Creates a plain object from a ServiceDescriptorProto message. Also converts values to other types if specified.
             * @param {google.protobuf.ServiceDescriptorProto} message ServiceDescriptorProto
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.ServiceDescriptorProto, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this ServiceDescriptorProto message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this ServiceDescriptorProto to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type MethodDescriptorProto$Properties = {
            name?: string;
            input_type?: string;
            output_type?: string;
            options?: google.protobuf.MethodOptions$Properties;
            client_streaming?: boolean;
            server_streaming?: boolean;
        };

        /**
         * Constructs a new MethodDescriptorProto.
         * @exports google.protobuf.MethodDescriptorProto
         * @constructor
         * @param {google.protobuf.MethodDescriptorProto$Properties=} [properties] Properties to set
         */
        class MethodDescriptorProto {

            /**
             * Constructs a new MethodDescriptorProto.
             * @exports google.protobuf.MethodDescriptorProto
             * @constructor
             * @param {google.protobuf.MethodDescriptorProto$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.MethodDescriptorProto$Properties);

            /**
             * MethodDescriptorProto name.
             * @type {string}
             */
            public name: string;

            /**
             * MethodDescriptorProto input_type.
             * @type {string}
             */
            public input_type: string;

            /**
             * MethodDescriptorProto output_type.
             * @type {string}
             */
            public output_type: string;

            /**
             * MethodDescriptorProto options.
             * @type {(google.protobuf.MethodOptions$Properties|null)}
             */
            public options: (google.protobuf.MethodOptions$Properties|null);

            /**
             * MethodDescriptorProto client_streaming.
             * @type {boolean}
             */
            public client_streaming: boolean;

            /**
             * MethodDescriptorProto server_streaming.
             * @type {boolean}
             */
            public server_streaming: boolean;

            /**
             * Creates a new MethodDescriptorProto instance using the specified properties.
             * @param {google.protobuf.MethodDescriptorProto$Properties=} [properties] Properties to set
             * @returns {google.protobuf.MethodDescriptorProto} MethodDescriptorProto instance
             */
            public static create(properties?: google.protobuf.MethodDescriptorProto$Properties): google.protobuf.MethodDescriptorProto;

            /**
             * Encodes the specified MethodDescriptorProto message. Does not implicitly {@link google.protobuf.MethodDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.MethodDescriptorProto$Properties} message MethodDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.MethodDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified MethodDescriptorProto message, length delimited. Does not implicitly {@link google.protobuf.MethodDescriptorProto.verify|verify} messages.
             * @param {google.protobuf.MethodDescriptorProto$Properties} message MethodDescriptorProto message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.MethodDescriptorProto$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a MethodDescriptorProto message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.MethodDescriptorProto} MethodDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.MethodDescriptorProto;

            /**
             * Decodes a MethodDescriptorProto message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.MethodDescriptorProto} MethodDescriptorProto
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.MethodDescriptorProto;

            /**
             * Verifies a MethodDescriptorProto message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a MethodDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.MethodDescriptorProto} MethodDescriptorProto
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.MethodDescriptorProto;

            /**
             * Creates a MethodDescriptorProto message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.MethodDescriptorProto.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.MethodDescriptorProto} MethodDescriptorProto
             */
            public static from(object: { [k: string]: any }): google.protobuf.MethodDescriptorProto;

            /**
             * Creates a plain object from a MethodDescriptorProto message. Also converts values to other types if specified.
             * @param {google.protobuf.MethodDescriptorProto} message MethodDescriptorProto
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.MethodDescriptorProto, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this MethodDescriptorProto message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this MethodDescriptorProto to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type FileOptions$Properties = {
            java_package?: string;
            java_outer_classname?: string;
            java_multiple_files?: boolean;
            java_generate_equals_and_hash?: boolean;
            java_string_check_utf8?: boolean;
            optimize_for?: google.protobuf.FileOptions.OptimizeMode;
            go_package?: string;
            cc_generic_services?: boolean;
            java_generic_services?: boolean;
            py_generic_services?: boolean;
            deprecated?: boolean;
            cc_enable_arenas?: boolean;
            objc_class_prefix?: string;
            csharp_namespace?: string;
            uninterpreted_option?: google.protobuf.UninterpretedOption$Properties[];
            ".gogoproto.goproto_getters_all"?: boolean;
            ".gogoproto.goproto_enum_prefix_all"?: boolean;
            ".gogoproto.goproto_stringer_all"?: boolean;
            ".gogoproto.verbose_equal_all"?: boolean;
            ".gogoproto.face_all"?: boolean;
            ".gogoproto.gostring_all"?: boolean;
            ".gogoproto.populate_all"?: boolean;
            ".gogoproto.stringer_all"?: boolean;
            ".gogoproto.onlyone_all"?: boolean;
            ".gogoproto.equal_all"?: boolean;
            ".gogoproto.description_all"?: boolean;
            ".gogoproto.testgen_all"?: boolean;
            ".gogoproto.benchgen_all"?: boolean;
            ".gogoproto.marshaler_all"?: boolean;
            ".gogoproto.unmarshaler_all"?: boolean;
            ".gogoproto.stable_marshaler_all"?: boolean;
            ".gogoproto.sizer_all"?: boolean;
            ".gogoproto.goproto_enum_stringer_all"?: boolean;
            ".gogoproto.enum_stringer_all"?: boolean;
            ".gogoproto.unsafe_marshaler_all"?: boolean;
            ".gogoproto.unsafe_unmarshaler_all"?: boolean;
            ".gogoproto.goproto_extensions_map_all"?: boolean;
            ".gogoproto.goproto_unrecognized_all"?: boolean;
            ".gogoproto.gogoproto_import"?: boolean;
            ".gogoproto.protosizer_all"?: boolean;
            ".gogoproto.compare_all"?: boolean;
            ".gogoproto.typedecl_all"?: boolean;
            ".gogoproto.enumdecl_all"?: boolean;
            ".gogoproto.goproto_registration"?: boolean;
        };

        /**
         * Constructs a new FileOptions.
         * @exports google.protobuf.FileOptions
         * @constructor
         * @param {google.protobuf.FileOptions$Properties=} [properties] Properties to set
         */
        class FileOptions {

            /**
             * Constructs a new FileOptions.
             * @exports google.protobuf.FileOptions
             * @constructor
             * @param {google.protobuf.FileOptions$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.FileOptions$Properties);

            /**
             * FileOptions java_package.
             * @type {string}
             */
            public java_package: string;

            /**
             * FileOptions java_outer_classname.
             * @type {string}
             */
            public java_outer_classname: string;

            /**
             * FileOptions java_multiple_files.
             * @type {boolean}
             */
            public java_multiple_files: boolean;

            /**
             * FileOptions java_generate_equals_and_hash.
             * @type {boolean}
             */
            public java_generate_equals_and_hash: boolean;

            /**
             * FileOptions java_string_check_utf8.
             * @type {boolean}
             */
            public java_string_check_utf8: boolean;

            /**
             * FileOptions optimize_for.
             * @type {google.protobuf.FileOptions.OptimizeMode}
             */
            public optimize_for: google.protobuf.FileOptions.OptimizeMode;

            /**
             * FileOptions go_package.
             * @type {string}
             */
            public go_package: string;

            /**
             * FileOptions cc_generic_services.
             * @type {boolean}
             */
            public cc_generic_services: boolean;

            /**
             * FileOptions java_generic_services.
             * @type {boolean}
             */
            public java_generic_services: boolean;

            /**
             * FileOptions py_generic_services.
             * @type {boolean}
             */
            public py_generic_services: boolean;

            /**
             * FileOptions deprecated.
             * @type {boolean}
             */
            public deprecated: boolean;

            /**
             * FileOptions cc_enable_arenas.
             * @type {boolean}
             */
            public cc_enable_arenas: boolean;

            /**
             * FileOptions objc_class_prefix.
             * @type {string}
             */
            public objc_class_prefix: string;

            /**
             * FileOptions csharp_namespace.
             * @type {string}
             */
            public csharp_namespace: string;

            /**
             * FileOptions uninterpreted_option.
             * @type {Array.<google.protobuf.UninterpretedOption$Properties>}
             */
            public uninterpreted_option: google.protobuf.UninterpretedOption$Properties[];

            /**
             * FileOptions .gogoproto.goproto_getters_all.
             * @type {boolean}
             */
            public [".gogoproto.goproto_getters_all"]: boolean;

            /**
             * FileOptions .gogoproto.goproto_enum_prefix_all.
             * @type {boolean}
             */
            public [".gogoproto.goproto_enum_prefix_all"]: boolean;

            /**
             * FileOptions .gogoproto.goproto_stringer_all.
             * @type {boolean}
             */
            public [".gogoproto.goproto_stringer_all"]: boolean;

            /**
             * FileOptions .gogoproto.verbose_equal_all.
             * @type {boolean}
             */
            public [".gogoproto.verbose_equal_all"]: boolean;

            /**
             * FileOptions .gogoproto.face_all.
             * @type {boolean}
             */
            public [".gogoproto.face_all"]: boolean;

            /**
             * FileOptions .gogoproto.gostring_all.
             * @type {boolean}
             */
            public [".gogoproto.gostring_all"]: boolean;

            /**
             * FileOptions .gogoproto.populate_all.
             * @type {boolean}
             */
            public [".gogoproto.populate_all"]: boolean;

            /**
             * FileOptions .gogoproto.stringer_all.
             * @type {boolean}
             */
            public [".gogoproto.stringer_all"]: boolean;

            /**
             * FileOptions .gogoproto.onlyone_all.
             * @type {boolean}
             */
            public [".gogoproto.onlyone_all"]: boolean;

            /**
             * FileOptions .gogoproto.equal_all.
             * @type {boolean}
             */
            public [".gogoproto.equal_all"]: boolean;

            /**
             * FileOptions .gogoproto.description_all.
             * @type {boolean}
             */
            public [".gogoproto.description_all"]: boolean;

            /**
             * FileOptions .gogoproto.testgen_all.
             * @type {boolean}
             */
            public [".gogoproto.testgen_all"]: boolean;

            /**
             * FileOptions .gogoproto.benchgen_all.
             * @type {boolean}
             */
            public [".gogoproto.benchgen_all"]: boolean;

            /**
             * FileOptions .gogoproto.marshaler_all.
             * @type {boolean}
             */
            public [".gogoproto.marshaler_all"]: boolean;

            /**
             * FileOptions .gogoproto.unmarshaler_all.
             * @type {boolean}
             */
            public [".gogoproto.unmarshaler_all"]: boolean;

            /**
             * FileOptions .gogoproto.stable_marshaler_all.
             * @type {boolean}
             */
            public [".gogoproto.stable_marshaler_all"]: boolean;

            /**
             * FileOptions .gogoproto.sizer_all.
             * @type {boolean}
             */
            public [".gogoproto.sizer_all"]: boolean;

            /**
             * FileOptions .gogoproto.goproto_enum_stringer_all.
             * @type {boolean}
             */
            public [".gogoproto.goproto_enum_stringer_all"]: boolean;

            /**
             * FileOptions .gogoproto.enum_stringer_all.
             * @type {boolean}
             */
            public [".gogoproto.enum_stringer_all"]: boolean;

            /**
             * FileOptions .gogoproto.unsafe_marshaler_all.
             * @type {boolean}
             */
            public [".gogoproto.unsafe_marshaler_all"]: boolean;

            /**
             * FileOptions .gogoproto.unsafe_unmarshaler_all.
             * @type {boolean}
             */
            public [".gogoproto.unsafe_unmarshaler_all"]: boolean;

            /**
             * FileOptions .gogoproto.goproto_extensions_map_all.
             * @type {boolean}
             */
            public [".gogoproto.goproto_extensions_map_all"]: boolean;

            /**
             * FileOptions .gogoproto.goproto_unrecognized_all.
             * @type {boolean}
             */
            public [".gogoproto.goproto_unrecognized_all"]: boolean;

            /**
             * FileOptions .gogoproto.gogoproto_import.
             * @type {boolean}
             */
            public [".gogoproto.gogoproto_import"]: boolean;

            /**
             * FileOptions .gogoproto.protosizer_all.
             * @type {boolean}
             */
            public [".gogoproto.protosizer_all"]: boolean;

            /**
             * FileOptions .gogoproto.compare_all.
             * @type {boolean}
             */
            public [".gogoproto.compare_all"]: boolean;

            /**
             * FileOptions .gogoproto.typedecl_all.
             * @type {boolean}
             */
            public [".gogoproto.typedecl_all"]: boolean;

            /**
             * FileOptions .gogoproto.enumdecl_all.
             * @type {boolean}
             */
            public [".gogoproto.enumdecl_all"]: boolean;

            /**
             * FileOptions .gogoproto.goproto_registration.
             * @type {boolean}
             */
            public [".gogoproto.goproto_registration"]: boolean;

            /**
             * Creates a new FileOptions instance using the specified properties.
             * @param {google.protobuf.FileOptions$Properties=} [properties] Properties to set
             * @returns {google.protobuf.FileOptions} FileOptions instance
             */
            public static create(properties?: google.protobuf.FileOptions$Properties): google.protobuf.FileOptions;

            /**
             * Encodes the specified FileOptions message. Does not implicitly {@link google.protobuf.FileOptions.verify|verify} messages.
             * @param {google.protobuf.FileOptions$Properties} message FileOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.FileOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified FileOptions message, length delimited. Does not implicitly {@link google.protobuf.FileOptions.verify|verify} messages.
             * @param {google.protobuf.FileOptions$Properties} message FileOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.FileOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a FileOptions message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.FileOptions} FileOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.FileOptions;

            /**
             * Decodes a FileOptions message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.FileOptions} FileOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.FileOptions;

            /**
             * Verifies a FileOptions message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a FileOptions message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.FileOptions} FileOptions
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.FileOptions;

            /**
             * Creates a FileOptions message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.FileOptions.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.FileOptions} FileOptions
             */
            public static from(object: { [k: string]: any }): google.protobuf.FileOptions;

            /**
             * Creates a plain object from a FileOptions message. Also converts values to other types if specified.
             * @param {google.protobuf.FileOptions} message FileOptions
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.FileOptions, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this FileOptions message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this FileOptions to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        namespace FileOptions {

            /**
             * OptimizeMode enum.
             * @name OptimizeMode
             * @memberof google.protobuf.FileOptions
             * @enum {number}
             * @property {number} SPEED=1 SPEED value
             * @property {number} CODE_SIZE=2 CODE_SIZE value
             * @property {number} LITE_RUNTIME=3 LITE_RUNTIME value
             */
            enum OptimizeMode {
                SPEED = 1,
                CODE_SIZE = 2,
                LITE_RUNTIME = 3
            }
        }

        type MessageOptions$Properties = {
            message_set_wire_format?: boolean;
            no_standard_descriptor_accessor?: boolean;
            deprecated?: boolean;
            map_entry?: boolean;
            uninterpreted_option?: google.protobuf.UninterpretedOption$Properties[];
            ".gogoproto.goproto_getters"?: boolean;
            ".gogoproto.goproto_stringer"?: boolean;
            ".gogoproto.verbose_equal"?: boolean;
            ".gogoproto.face"?: boolean;
            ".gogoproto.gostring"?: boolean;
            ".gogoproto.populate"?: boolean;
            ".gogoproto.stringer"?: boolean;
            ".gogoproto.onlyone"?: boolean;
            ".gogoproto.equal"?: boolean;
            ".gogoproto.description"?: boolean;
            ".gogoproto.testgen"?: boolean;
            ".gogoproto.benchgen"?: boolean;
            ".gogoproto.marshaler"?: boolean;
            ".gogoproto.unmarshaler"?: boolean;
            ".gogoproto.stable_marshaler"?: boolean;
            ".gogoproto.sizer"?: boolean;
            ".gogoproto.unsafe_marshaler"?: boolean;
            ".gogoproto.unsafe_unmarshaler"?: boolean;
            ".gogoproto.goproto_extensions_map"?: boolean;
            ".gogoproto.goproto_unrecognized"?: boolean;
            ".gogoproto.protosizer"?: boolean;
            ".gogoproto.compare"?: boolean;
            ".gogoproto.typedecl"?: boolean;
        };

        /**
         * Constructs a new MessageOptions.
         * @exports google.protobuf.MessageOptions
         * @constructor
         * @param {google.protobuf.MessageOptions$Properties=} [properties] Properties to set
         */
        class MessageOptions {

            /**
             * Constructs a new MessageOptions.
             * @exports google.protobuf.MessageOptions
             * @constructor
             * @param {google.protobuf.MessageOptions$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.MessageOptions$Properties);

            /**
             * MessageOptions message_set_wire_format.
             * @type {boolean}
             */
            public message_set_wire_format: boolean;

            /**
             * MessageOptions no_standard_descriptor_accessor.
             * @type {boolean}
             */
            public no_standard_descriptor_accessor: boolean;

            /**
             * MessageOptions deprecated.
             * @type {boolean}
             */
            public deprecated: boolean;

            /**
             * MessageOptions map_entry.
             * @type {boolean}
             */
            public map_entry: boolean;

            /**
             * MessageOptions uninterpreted_option.
             * @type {Array.<google.protobuf.UninterpretedOption$Properties>}
             */
            public uninterpreted_option: google.protobuf.UninterpretedOption$Properties[];

            /**
             * MessageOptions .gogoproto.goproto_getters.
             * @type {boolean}
             */
            public [".gogoproto.goproto_getters"]: boolean;

            /**
             * MessageOptions .gogoproto.goproto_stringer.
             * @type {boolean}
             */
            public [".gogoproto.goproto_stringer"]: boolean;

            /**
             * MessageOptions .gogoproto.verbose_equal.
             * @type {boolean}
             */
            public [".gogoproto.verbose_equal"]: boolean;

            /**
             * MessageOptions .gogoproto.face.
             * @type {boolean}
             */
            public [".gogoproto.face"]: boolean;

            /**
             * MessageOptions .gogoproto.gostring.
             * @type {boolean}
             */
            public [".gogoproto.gostring"]: boolean;

            /**
             * MessageOptions .gogoproto.populate.
             * @type {boolean}
             */
            public [".gogoproto.populate"]: boolean;

            /**
             * MessageOptions .gogoproto.stringer.
             * @type {boolean}
             */
            public [".gogoproto.stringer"]: boolean;

            /**
             * MessageOptions .gogoproto.onlyone.
             * @type {boolean}
             */
            public [".gogoproto.onlyone"]: boolean;

            /**
             * MessageOptions .gogoproto.equal.
             * @type {boolean}
             */
            public [".gogoproto.equal"]: boolean;

            /**
             * MessageOptions .gogoproto.description.
             * @type {boolean}
             */
            public [".gogoproto.description"]: boolean;

            /**
             * MessageOptions .gogoproto.testgen.
             * @type {boolean}
             */
            public [".gogoproto.testgen"]: boolean;

            /**
             * MessageOptions .gogoproto.benchgen.
             * @type {boolean}
             */
            public [".gogoproto.benchgen"]: boolean;

            /**
             * MessageOptions .gogoproto.marshaler.
             * @type {boolean}
             */
            public [".gogoproto.marshaler"]: boolean;

            /**
             * MessageOptions .gogoproto.unmarshaler.
             * @type {boolean}
             */
            public [".gogoproto.unmarshaler"]: boolean;

            /**
             * MessageOptions .gogoproto.stable_marshaler.
             * @type {boolean}
             */
            public [".gogoproto.stable_marshaler"]: boolean;

            /**
             * MessageOptions .gogoproto.sizer.
             * @type {boolean}
             */
            public [".gogoproto.sizer"]: boolean;

            /**
             * MessageOptions .gogoproto.unsafe_marshaler.
             * @type {boolean}
             */
            public [".gogoproto.unsafe_marshaler"]: boolean;

            /**
             * MessageOptions .gogoproto.unsafe_unmarshaler.
             * @type {boolean}
             */
            public [".gogoproto.unsafe_unmarshaler"]: boolean;

            /**
             * MessageOptions .gogoproto.goproto_extensions_map.
             * @type {boolean}
             */
            public [".gogoproto.goproto_extensions_map"]: boolean;

            /**
             * MessageOptions .gogoproto.goproto_unrecognized.
             * @type {boolean}
             */
            public [".gogoproto.goproto_unrecognized"]: boolean;

            /**
             * MessageOptions .gogoproto.protosizer.
             * @type {boolean}
             */
            public [".gogoproto.protosizer"]: boolean;

            /**
             * MessageOptions .gogoproto.compare.
             * @type {boolean}
             */
            public [".gogoproto.compare"]: boolean;

            /**
             * MessageOptions .gogoproto.typedecl.
             * @type {boolean}
             */
            public [".gogoproto.typedecl"]: boolean;

            /**
             * Creates a new MessageOptions instance using the specified properties.
             * @param {google.protobuf.MessageOptions$Properties=} [properties] Properties to set
             * @returns {google.protobuf.MessageOptions} MessageOptions instance
             */
            public static create(properties?: google.protobuf.MessageOptions$Properties): google.protobuf.MessageOptions;

            /**
             * Encodes the specified MessageOptions message. Does not implicitly {@link google.protobuf.MessageOptions.verify|verify} messages.
             * @param {google.protobuf.MessageOptions$Properties} message MessageOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.MessageOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified MessageOptions message, length delimited. Does not implicitly {@link google.protobuf.MessageOptions.verify|verify} messages.
             * @param {google.protobuf.MessageOptions$Properties} message MessageOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.MessageOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a MessageOptions message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.MessageOptions} MessageOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.MessageOptions;

            /**
             * Decodes a MessageOptions message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.MessageOptions} MessageOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.MessageOptions;

            /**
             * Verifies a MessageOptions message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a MessageOptions message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.MessageOptions} MessageOptions
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.MessageOptions;

            /**
             * Creates a MessageOptions message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.MessageOptions.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.MessageOptions} MessageOptions
             */
            public static from(object: { [k: string]: any }): google.protobuf.MessageOptions;

            /**
             * Creates a plain object from a MessageOptions message. Also converts values to other types if specified.
             * @param {google.protobuf.MessageOptions} message MessageOptions
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.MessageOptions, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this MessageOptions message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this MessageOptions to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type FieldOptions$Properties = {
            ctype?: google.protobuf.FieldOptions.CType;
            packed?: boolean;
            jstype?: google.protobuf.FieldOptions.JSType;
            lazy?: boolean;
            deprecated?: boolean;
            weak?: boolean;
            uninterpreted_option?: google.protobuf.UninterpretedOption$Properties[];
            ".gogoproto.nullable"?: boolean;
            ".gogoproto.embed"?: boolean;
            ".gogoproto.customtype"?: string;
            ".gogoproto.customname"?: string;
            ".gogoproto.jsontag"?: string;
            ".gogoproto.moretags"?: string;
            ".gogoproto.casttype"?: string;
            ".gogoproto.castkey"?: string;
            ".gogoproto.castvalue"?: string;
            ".gogoproto.stdtime"?: boolean;
            ".gogoproto.stdduration"?: boolean;
        };

        /**
         * Constructs a new FieldOptions.
         * @exports google.protobuf.FieldOptions
         * @constructor
         * @param {google.protobuf.FieldOptions$Properties=} [properties] Properties to set
         */
        class FieldOptions {

            /**
             * Constructs a new FieldOptions.
             * @exports google.protobuf.FieldOptions
             * @constructor
             * @param {google.protobuf.FieldOptions$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.FieldOptions$Properties);

            /**
             * FieldOptions ctype.
             * @type {google.protobuf.FieldOptions.CType}
             */
            public ctype: google.protobuf.FieldOptions.CType;

            /**
             * FieldOptions packed.
             * @type {boolean}
             */
            public packed: boolean;

            /**
             * FieldOptions jstype.
             * @type {google.protobuf.FieldOptions.JSType}
             */
            public jstype: google.protobuf.FieldOptions.JSType;

            /**
             * FieldOptions lazy.
             * @type {boolean}
             */
            public lazy: boolean;

            /**
             * FieldOptions deprecated.
             * @type {boolean}
             */
            public deprecated: boolean;

            /**
             * FieldOptions weak.
             * @type {boolean}
             */
            public weak: boolean;

            /**
             * FieldOptions uninterpreted_option.
             * @type {Array.<google.protobuf.UninterpretedOption$Properties>}
             */
            public uninterpreted_option: google.protobuf.UninterpretedOption$Properties[];

            /**
             * FieldOptions .gogoproto.nullable.
             * @type {boolean}
             */
            public [".gogoproto.nullable"]: boolean;

            /**
             * FieldOptions .gogoproto.embed.
             * @type {boolean}
             */
            public [".gogoproto.embed"]: boolean;

            /**
             * FieldOptions .gogoproto.customtype.
             * @type {string}
             */
            public [".gogoproto.customtype"]: string;

            /**
             * FieldOptions .gogoproto.customname.
             * @type {string}
             */
            public [".gogoproto.customname"]: string;

            /**
             * FieldOptions .gogoproto.jsontag.
             * @type {string}
             */
            public [".gogoproto.jsontag"]: string;

            /**
             * FieldOptions .gogoproto.moretags.
             * @type {string}
             */
            public [".gogoproto.moretags"]: string;

            /**
             * FieldOptions .gogoproto.casttype.
             * @type {string}
             */
            public [".gogoproto.casttype"]: string;

            /**
             * FieldOptions .gogoproto.castkey.
             * @type {string}
             */
            public [".gogoproto.castkey"]: string;

            /**
             * FieldOptions .gogoproto.castvalue.
             * @type {string}
             */
            public [".gogoproto.castvalue"]: string;

            /**
             * FieldOptions .gogoproto.stdtime.
             * @type {boolean}
             */
            public [".gogoproto.stdtime"]: boolean;

            /**
             * FieldOptions .gogoproto.stdduration.
             * @type {boolean}
             */
            public [".gogoproto.stdduration"]: boolean;

            /**
             * Creates a new FieldOptions instance using the specified properties.
             * @param {google.protobuf.FieldOptions$Properties=} [properties] Properties to set
             * @returns {google.protobuf.FieldOptions} FieldOptions instance
             */
            public static create(properties?: google.protobuf.FieldOptions$Properties): google.protobuf.FieldOptions;

            /**
             * Encodes the specified FieldOptions message. Does not implicitly {@link google.protobuf.FieldOptions.verify|verify} messages.
             * @param {google.protobuf.FieldOptions$Properties} message FieldOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.FieldOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified FieldOptions message, length delimited. Does not implicitly {@link google.protobuf.FieldOptions.verify|verify} messages.
             * @param {google.protobuf.FieldOptions$Properties} message FieldOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.FieldOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a FieldOptions message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.FieldOptions} FieldOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.FieldOptions;

            /**
             * Decodes a FieldOptions message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.FieldOptions} FieldOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.FieldOptions;

            /**
             * Verifies a FieldOptions message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a FieldOptions message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.FieldOptions} FieldOptions
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.FieldOptions;

            /**
             * Creates a FieldOptions message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.FieldOptions.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.FieldOptions} FieldOptions
             */
            public static from(object: { [k: string]: any }): google.protobuf.FieldOptions;

            /**
             * Creates a plain object from a FieldOptions message. Also converts values to other types if specified.
             * @param {google.protobuf.FieldOptions} message FieldOptions
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.FieldOptions, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this FieldOptions message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this FieldOptions to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        namespace FieldOptions {

            /**
             * CType enum.
             * @name CType
             * @memberof google.protobuf.FieldOptions
             * @enum {number}
             * @property {number} STRING=0 STRING value
             * @property {number} CORD=1 CORD value
             * @property {number} STRING_PIECE=2 STRING_PIECE value
             */
            enum CType {
                STRING = 0,
                CORD = 1,
                STRING_PIECE = 2
            }

            /**
             * JSType enum.
             * @name JSType
             * @memberof google.protobuf.FieldOptions
             * @enum {number}
             * @property {number} JS_NORMAL=0 JS_NORMAL value
             * @property {number} JS_STRING=1 JS_STRING value
             * @property {number} JS_NUMBER=2 JS_NUMBER value
             */
            enum JSType {
                JS_NORMAL = 0,
                JS_STRING = 1,
                JS_NUMBER = 2
            }
        }

        type OneofOptions$Properties = {
            uninterpreted_option?: google.protobuf.UninterpretedOption$Properties[];
        };

        /**
         * Constructs a new OneofOptions.
         * @exports google.protobuf.OneofOptions
         * @constructor
         * @param {google.protobuf.OneofOptions$Properties=} [properties] Properties to set
         */
        class OneofOptions {

            /**
             * Constructs a new OneofOptions.
             * @exports google.protobuf.OneofOptions
             * @constructor
             * @param {google.protobuf.OneofOptions$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.OneofOptions$Properties);

            /**
             * OneofOptions uninterpreted_option.
             * @type {Array.<google.protobuf.UninterpretedOption$Properties>}
             */
            public uninterpreted_option: google.protobuf.UninterpretedOption$Properties[];

            /**
             * Creates a new OneofOptions instance using the specified properties.
             * @param {google.protobuf.OneofOptions$Properties=} [properties] Properties to set
             * @returns {google.protobuf.OneofOptions} OneofOptions instance
             */
            public static create(properties?: google.protobuf.OneofOptions$Properties): google.protobuf.OneofOptions;

            /**
             * Encodes the specified OneofOptions message. Does not implicitly {@link google.protobuf.OneofOptions.verify|verify} messages.
             * @param {google.protobuf.OneofOptions$Properties} message OneofOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.OneofOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified OneofOptions message, length delimited. Does not implicitly {@link google.protobuf.OneofOptions.verify|verify} messages.
             * @param {google.protobuf.OneofOptions$Properties} message OneofOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.OneofOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes an OneofOptions message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.OneofOptions} OneofOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.OneofOptions;

            /**
             * Decodes an OneofOptions message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.OneofOptions} OneofOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.OneofOptions;

            /**
             * Verifies an OneofOptions message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates an OneofOptions message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.OneofOptions} OneofOptions
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.OneofOptions;

            /**
             * Creates an OneofOptions message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.OneofOptions.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.OneofOptions} OneofOptions
             */
            public static from(object: { [k: string]: any }): google.protobuf.OneofOptions;

            /**
             * Creates a plain object from an OneofOptions message. Also converts values to other types if specified.
             * @param {google.protobuf.OneofOptions} message OneofOptions
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.OneofOptions, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this OneofOptions message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this OneofOptions to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type EnumOptions$Properties = {
            allow_alias?: boolean;
            deprecated?: boolean;
            uninterpreted_option?: google.protobuf.UninterpretedOption$Properties[];
            ".gogoproto.goproto_enum_prefix"?: boolean;
            ".gogoproto.goproto_enum_stringer"?: boolean;
            ".gogoproto.enum_stringer"?: boolean;
            ".gogoproto.enum_customname"?: string;
            ".gogoproto.enumdecl"?: boolean;
        };

        /**
         * Constructs a new EnumOptions.
         * @exports google.protobuf.EnumOptions
         * @constructor
         * @param {google.protobuf.EnumOptions$Properties=} [properties] Properties to set
         */
        class EnumOptions {

            /**
             * Constructs a new EnumOptions.
             * @exports google.protobuf.EnumOptions
             * @constructor
             * @param {google.protobuf.EnumOptions$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.EnumOptions$Properties);

            /**
             * EnumOptions allow_alias.
             * @type {boolean}
             */
            public allow_alias: boolean;

            /**
             * EnumOptions deprecated.
             * @type {boolean}
             */
            public deprecated: boolean;

            /**
             * EnumOptions uninterpreted_option.
             * @type {Array.<google.protobuf.UninterpretedOption$Properties>}
             */
            public uninterpreted_option: google.protobuf.UninterpretedOption$Properties[];

            /**
             * EnumOptions .gogoproto.goproto_enum_prefix.
             * @type {boolean}
             */
            public [".gogoproto.goproto_enum_prefix"]: boolean;

            /**
             * EnumOptions .gogoproto.goproto_enum_stringer.
             * @type {boolean}
             */
            public [".gogoproto.goproto_enum_stringer"]: boolean;

            /**
             * EnumOptions .gogoproto.enum_stringer.
             * @type {boolean}
             */
            public [".gogoproto.enum_stringer"]: boolean;

            /**
             * EnumOptions .gogoproto.enum_customname.
             * @type {string}
             */
            public [".gogoproto.enum_customname"]: string;

            /**
             * EnumOptions .gogoproto.enumdecl.
             * @type {boolean}
             */
            public [".gogoproto.enumdecl"]: boolean;

            /**
             * Creates a new EnumOptions instance using the specified properties.
             * @param {google.protobuf.EnumOptions$Properties=} [properties] Properties to set
             * @returns {google.protobuf.EnumOptions} EnumOptions instance
             */
            public static create(properties?: google.protobuf.EnumOptions$Properties): google.protobuf.EnumOptions;

            /**
             * Encodes the specified EnumOptions message. Does not implicitly {@link google.protobuf.EnumOptions.verify|verify} messages.
             * @param {google.protobuf.EnumOptions$Properties} message EnumOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.EnumOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified EnumOptions message, length delimited. Does not implicitly {@link google.protobuf.EnumOptions.verify|verify} messages.
             * @param {google.protobuf.EnumOptions$Properties} message EnumOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.EnumOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes an EnumOptions message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.EnumOptions} EnumOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.EnumOptions;

            /**
             * Decodes an EnumOptions message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.EnumOptions} EnumOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.EnumOptions;

            /**
             * Verifies an EnumOptions message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates an EnumOptions message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.EnumOptions} EnumOptions
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.EnumOptions;

            /**
             * Creates an EnumOptions message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.EnumOptions.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.EnumOptions} EnumOptions
             */
            public static from(object: { [k: string]: any }): google.protobuf.EnumOptions;

            /**
             * Creates a plain object from an EnumOptions message. Also converts values to other types if specified.
             * @param {google.protobuf.EnumOptions} message EnumOptions
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.EnumOptions, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this EnumOptions message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this EnumOptions to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type EnumValueOptions$Properties = {
            deprecated?: boolean;
            uninterpreted_option?: google.protobuf.UninterpretedOption$Properties[];
            ".gogoproto.enumvalue_customname"?: string;
        };

        /**
         * Constructs a new EnumValueOptions.
         * @exports google.protobuf.EnumValueOptions
         * @constructor
         * @param {google.protobuf.EnumValueOptions$Properties=} [properties] Properties to set
         */
        class EnumValueOptions {

            /**
             * Constructs a new EnumValueOptions.
             * @exports google.protobuf.EnumValueOptions
             * @constructor
             * @param {google.protobuf.EnumValueOptions$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.EnumValueOptions$Properties);

            /**
             * EnumValueOptions deprecated.
             * @type {boolean}
             */
            public deprecated: boolean;

            /**
             * EnumValueOptions uninterpreted_option.
             * @type {Array.<google.protobuf.UninterpretedOption$Properties>}
             */
            public uninterpreted_option: google.protobuf.UninterpretedOption$Properties[];

            /**
             * EnumValueOptions .gogoproto.enumvalue_customname.
             * @type {string}
             */
            public [".gogoproto.enumvalue_customname"]: string;

            /**
             * Creates a new EnumValueOptions instance using the specified properties.
             * @param {google.protobuf.EnumValueOptions$Properties=} [properties] Properties to set
             * @returns {google.protobuf.EnumValueOptions} EnumValueOptions instance
             */
            public static create(properties?: google.protobuf.EnumValueOptions$Properties): google.protobuf.EnumValueOptions;

            /**
             * Encodes the specified EnumValueOptions message. Does not implicitly {@link google.protobuf.EnumValueOptions.verify|verify} messages.
             * @param {google.protobuf.EnumValueOptions$Properties} message EnumValueOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.EnumValueOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified EnumValueOptions message, length delimited. Does not implicitly {@link google.protobuf.EnumValueOptions.verify|verify} messages.
             * @param {google.protobuf.EnumValueOptions$Properties} message EnumValueOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.EnumValueOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes an EnumValueOptions message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.EnumValueOptions} EnumValueOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.EnumValueOptions;

            /**
             * Decodes an EnumValueOptions message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.EnumValueOptions} EnumValueOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.EnumValueOptions;

            /**
             * Verifies an EnumValueOptions message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates an EnumValueOptions message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.EnumValueOptions} EnumValueOptions
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.EnumValueOptions;

            /**
             * Creates an EnumValueOptions message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.EnumValueOptions.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.EnumValueOptions} EnumValueOptions
             */
            public static from(object: { [k: string]: any }): google.protobuf.EnumValueOptions;

            /**
             * Creates a plain object from an EnumValueOptions message. Also converts values to other types if specified.
             * @param {google.protobuf.EnumValueOptions} message EnumValueOptions
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.EnumValueOptions, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this EnumValueOptions message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this EnumValueOptions to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type ServiceOptions$Properties = {
            deprecated?: boolean;
            uninterpreted_option?: google.protobuf.UninterpretedOption$Properties[];
        };

        /**
         * Constructs a new ServiceOptions.
         * @exports google.protobuf.ServiceOptions
         * @constructor
         * @param {google.protobuf.ServiceOptions$Properties=} [properties] Properties to set
         */
        class ServiceOptions {

            /**
             * Constructs a new ServiceOptions.
             * @exports google.protobuf.ServiceOptions
             * @constructor
             * @param {google.protobuf.ServiceOptions$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.ServiceOptions$Properties);

            /**
             * ServiceOptions deprecated.
             * @type {boolean}
             */
            public deprecated: boolean;

            /**
             * ServiceOptions uninterpreted_option.
             * @type {Array.<google.protobuf.UninterpretedOption$Properties>}
             */
            public uninterpreted_option: google.protobuf.UninterpretedOption$Properties[];

            /**
             * Creates a new ServiceOptions instance using the specified properties.
             * @param {google.protobuf.ServiceOptions$Properties=} [properties] Properties to set
             * @returns {google.protobuf.ServiceOptions} ServiceOptions instance
             */
            public static create(properties?: google.protobuf.ServiceOptions$Properties): google.protobuf.ServiceOptions;

            /**
             * Encodes the specified ServiceOptions message. Does not implicitly {@link google.protobuf.ServiceOptions.verify|verify} messages.
             * @param {google.protobuf.ServiceOptions$Properties} message ServiceOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.ServiceOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified ServiceOptions message, length delimited. Does not implicitly {@link google.protobuf.ServiceOptions.verify|verify} messages.
             * @param {google.protobuf.ServiceOptions$Properties} message ServiceOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.ServiceOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a ServiceOptions message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.ServiceOptions} ServiceOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.ServiceOptions;

            /**
             * Decodes a ServiceOptions message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.ServiceOptions} ServiceOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.ServiceOptions;

            /**
             * Verifies a ServiceOptions message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a ServiceOptions message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.ServiceOptions} ServiceOptions
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.ServiceOptions;

            /**
             * Creates a ServiceOptions message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.ServiceOptions.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.ServiceOptions} ServiceOptions
             */
            public static from(object: { [k: string]: any }): google.protobuf.ServiceOptions;

            /**
             * Creates a plain object from a ServiceOptions message. Also converts values to other types if specified.
             * @param {google.protobuf.ServiceOptions} message ServiceOptions
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.ServiceOptions, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this ServiceOptions message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this ServiceOptions to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type MethodOptions$Properties = {
            deprecated?: boolean;
            uninterpreted_option?: google.protobuf.UninterpretedOption$Properties[];
        };

        /**
         * Constructs a new MethodOptions.
         * @exports google.protobuf.MethodOptions
         * @constructor
         * @param {google.protobuf.MethodOptions$Properties=} [properties] Properties to set
         */
        class MethodOptions {

            /**
             * Constructs a new MethodOptions.
             * @exports google.protobuf.MethodOptions
             * @constructor
             * @param {google.protobuf.MethodOptions$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.MethodOptions$Properties);

            /**
             * MethodOptions deprecated.
             * @type {boolean}
             */
            public deprecated: boolean;

            /**
             * MethodOptions uninterpreted_option.
             * @type {Array.<google.protobuf.UninterpretedOption$Properties>}
             */
            public uninterpreted_option: google.protobuf.UninterpretedOption$Properties[];

            /**
             * Creates a new MethodOptions instance using the specified properties.
             * @param {google.protobuf.MethodOptions$Properties=} [properties] Properties to set
             * @returns {google.protobuf.MethodOptions} MethodOptions instance
             */
            public static create(properties?: google.protobuf.MethodOptions$Properties): google.protobuf.MethodOptions;

            /**
             * Encodes the specified MethodOptions message. Does not implicitly {@link google.protobuf.MethodOptions.verify|verify} messages.
             * @param {google.protobuf.MethodOptions$Properties} message MethodOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.MethodOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified MethodOptions message, length delimited. Does not implicitly {@link google.protobuf.MethodOptions.verify|verify} messages.
             * @param {google.protobuf.MethodOptions$Properties} message MethodOptions message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.MethodOptions$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a MethodOptions message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.MethodOptions} MethodOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.MethodOptions;

            /**
             * Decodes a MethodOptions message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.MethodOptions} MethodOptions
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.MethodOptions;

            /**
             * Verifies a MethodOptions message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a MethodOptions message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.MethodOptions} MethodOptions
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.MethodOptions;

            /**
             * Creates a MethodOptions message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.MethodOptions.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.MethodOptions} MethodOptions
             */
            public static from(object: { [k: string]: any }): google.protobuf.MethodOptions;

            /**
             * Creates a plain object from a MethodOptions message. Also converts values to other types if specified.
             * @param {google.protobuf.MethodOptions} message MethodOptions
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.MethodOptions, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this MethodOptions message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this MethodOptions to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        type UninterpretedOption$Properties = {
            name?: google.protobuf.UninterpretedOption.NamePart$Properties[];
            identifier_value?: string;
            positive_int_value?: Long;
            negative_int_value?: Long;
            double_value?: number;
            string_value?: Uint8Array;
            aggregate_value?: string;
        };

        /**
         * Constructs a new UninterpretedOption.
         * @exports google.protobuf.UninterpretedOption
         * @constructor
         * @param {google.protobuf.UninterpretedOption$Properties=} [properties] Properties to set
         */
        class UninterpretedOption {

            /**
             * Constructs a new UninterpretedOption.
             * @exports google.protobuf.UninterpretedOption
             * @constructor
             * @param {google.protobuf.UninterpretedOption$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.UninterpretedOption$Properties);

            /**
             * UninterpretedOption name.
             * @type {Array.<google.protobuf.UninterpretedOption.NamePart$Properties>}
             */
            public name: google.protobuf.UninterpretedOption.NamePart$Properties[];

            /**
             * UninterpretedOption identifier_value.
             * @type {string}
             */
            public identifier_value: string;

            /**
             * UninterpretedOption positive_int_value.
             * @type {Long}
             */
            public positive_int_value: Long;

            /**
             * UninterpretedOption negative_int_value.
             * @type {Long}
             */
            public negative_int_value: Long;

            /**
             * UninterpretedOption double_value.
             * @type {number}
             */
            public double_value: number;

            /**
             * UninterpretedOption string_value.
             * @type {Uint8Array}
             */
            public string_value: Uint8Array;

            /**
             * UninterpretedOption aggregate_value.
             * @type {string}
             */
            public aggregate_value: string;

            /**
             * Creates a new UninterpretedOption instance using the specified properties.
             * @param {google.protobuf.UninterpretedOption$Properties=} [properties] Properties to set
             * @returns {google.protobuf.UninterpretedOption} UninterpretedOption instance
             */
            public static create(properties?: google.protobuf.UninterpretedOption$Properties): google.protobuf.UninterpretedOption;

            /**
             * Encodes the specified UninterpretedOption message. Does not implicitly {@link google.protobuf.UninterpretedOption.verify|verify} messages.
             * @param {google.protobuf.UninterpretedOption$Properties} message UninterpretedOption message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.UninterpretedOption$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified UninterpretedOption message, length delimited. Does not implicitly {@link google.protobuf.UninterpretedOption.verify|verify} messages.
             * @param {google.protobuf.UninterpretedOption$Properties} message UninterpretedOption message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.UninterpretedOption$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes an UninterpretedOption message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.UninterpretedOption} UninterpretedOption
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.UninterpretedOption;

            /**
             * Decodes an UninterpretedOption message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.UninterpretedOption} UninterpretedOption
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.UninterpretedOption;

            /**
             * Verifies an UninterpretedOption message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates an UninterpretedOption message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.UninterpretedOption} UninterpretedOption
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.UninterpretedOption;

            /**
             * Creates an UninterpretedOption message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.UninterpretedOption.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.UninterpretedOption} UninterpretedOption
             */
            public static from(object: { [k: string]: any }): google.protobuf.UninterpretedOption;

            /**
             * Creates a plain object from an UninterpretedOption message. Also converts values to other types if specified.
             * @param {google.protobuf.UninterpretedOption} message UninterpretedOption
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.UninterpretedOption, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this UninterpretedOption message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this UninterpretedOption to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        namespace UninterpretedOption {

            type NamePart$Properties = {
                name_part: string;
                is_extension: boolean;
            };

            /**
             * Constructs a new NamePart.
             * @exports google.protobuf.UninterpretedOption.NamePart
             * @constructor
             * @param {google.protobuf.UninterpretedOption.NamePart$Properties=} [properties] Properties to set
             */
            class NamePart {

                /**
                 * Constructs a new NamePart.
                 * @exports google.protobuf.UninterpretedOption.NamePart
                 * @constructor
                 * @param {google.protobuf.UninterpretedOption.NamePart$Properties=} [properties] Properties to set
                 */
                constructor(properties?: google.protobuf.UninterpretedOption.NamePart$Properties);

                /**
                 * NamePart name_part.
                 * @type {string}
                 */
                public name_part: string;

                /**
                 * NamePart is_extension.
                 * @type {boolean}
                 */
                public is_extension: boolean;

                /**
                 * Creates a new NamePart instance using the specified properties.
                 * @param {google.protobuf.UninterpretedOption.NamePart$Properties=} [properties] Properties to set
                 * @returns {google.protobuf.UninterpretedOption.NamePart} NamePart instance
                 */
                public static create(properties?: google.protobuf.UninterpretedOption.NamePart$Properties): google.protobuf.UninterpretedOption.NamePart;

                /**
                 * Encodes the specified NamePart message. Does not implicitly {@link google.protobuf.UninterpretedOption.NamePart.verify|verify} messages.
                 * @param {google.protobuf.UninterpretedOption.NamePart$Properties} message NamePart message or plain object to encode
                 * @param {$protobuf.Writer} [writer] Writer to encode to
                 * @returns {$protobuf.Writer} Writer
                 */
                public static encode(message: google.protobuf.UninterpretedOption.NamePart$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                /**
                 * Encodes the specified NamePart message, length delimited. Does not implicitly {@link google.protobuf.UninterpretedOption.NamePart.verify|verify} messages.
                 * @param {google.protobuf.UninterpretedOption.NamePart$Properties} message NamePart message or plain object to encode
                 * @param {$protobuf.Writer} [writer] Writer to encode to
                 * @returns {$protobuf.Writer} Writer
                 */
                public static encodeDelimited(message: google.protobuf.UninterpretedOption.NamePart$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                /**
                 * Decodes a NamePart message from the specified reader or buffer.
                 * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                 * @param {number} [length] Message length if known beforehand
                 * @returns {google.protobuf.UninterpretedOption.NamePart} NamePart
                 * @throws {Error} If the payload is not a reader or valid buffer
                 * @throws {$protobuf.util.ProtocolError} If required fields are missing
                 */
                public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.UninterpretedOption.NamePart;

                /**
                 * Decodes a NamePart message from the specified reader or buffer, length delimited.
                 * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                 * @returns {google.protobuf.UninterpretedOption.NamePart} NamePart
                 * @throws {Error} If the payload is not a reader or valid buffer
                 * @throws {$protobuf.util.ProtocolError} If required fields are missing
                 */
                public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.UninterpretedOption.NamePart;

                /**
                 * Verifies a NamePart message.
                 * @param {Object.<string,*>} message Plain object to verify
                 * @returns {?string} `null` if valid, otherwise the reason why it is not
                 */
                public static verify(message: { [k: string]: any }): string;

                /**
                 * Creates a NamePart message from a plain object. Also converts values to their respective internal types.
                 * @param {Object.<string,*>} object Plain object
                 * @returns {google.protobuf.UninterpretedOption.NamePart} NamePart
                 */
                public static fromObject(object: { [k: string]: any }): google.protobuf.UninterpretedOption.NamePart;

                /**
                 * Creates a NamePart message from a plain object. Also converts values to their respective internal types.
                 * This is an alias of {@link google.protobuf.UninterpretedOption.NamePart.fromObject}.
                 * @function
                 * @param {Object.<string,*>} object Plain object
                 * @returns {google.protobuf.UninterpretedOption.NamePart} NamePart
                 */
                public static from(object: { [k: string]: any }): google.protobuf.UninterpretedOption.NamePart;

                /**
                 * Creates a plain object from a NamePart message. Also converts values to other types if specified.
                 * @param {google.protobuf.UninterpretedOption.NamePart} message NamePart
                 * @param {$protobuf.ConversionOptions} [options] Conversion options
                 * @returns {Object.<string,*>} Plain object
                 */
                public static toObject(message: google.protobuf.UninterpretedOption.NamePart, options?: $protobuf.ConversionOptions): { [k: string]: any };

                /**
                 * Creates a plain object from this NamePart message. Also converts values to other types if specified.
                 * @param {$protobuf.ConversionOptions} [options] Conversion options
                 * @returns {Object.<string,*>} Plain object
                 */
                public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

                /**
                 * Converts this NamePart to JSON.
                 * @returns {Object.<string,*>} JSON object
                 */
                public toJSON(): { [k: string]: any };
            }
        }

        type SourceCodeInfo$Properties = {
            location?: google.protobuf.SourceCodeInfo.Location$Properties[];
        };

        /**
         * Constructs a new SourceCodeInfo.
         * @exports google.protobuf.SourceCodeInfo
         * @constructor
         * @param {google.protobuf.SourceCodeInfo$Properties=} [properties] Properties to set
         */
        class SourceCodeInfo {

            /**
             * Constructs a new SourceCodeInfo.
             * @exports google.protobuf.SourceCodeInfo
             * @constructor
             * @param {google.protobuf.SourceCodeInfo$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.SourceCodeInfo$Properties);

            /**
             * SourceCodeInfo location.
             * @type {Array.<google.protobuf.SourceCodeInfo.Location$Properties>}
             */
            public location: google.protobuf.SourceCodeInfo.Location$Properties[];

            /**
             * Creates a new SourceCodeInfo instance using the specified properties.
             * @param {google.protobuf.SourceCodeInfo$Properties=} [properties] Properties to set
             * @returns {google.protobuf.SourceCodeInfo} SourceCodeInfo instance
             */
            public static create(properties?: google.protobuf.SourceCodeInfo$Properties): google.protobuf.SourceCodeInfo;

            /**
             * Encodes the specified SourceCodeInfo message. Does not implicitly {@link google.protobuf.SourceCodeInfo.verify|verify} messages.
             * @param {google.protobuf.SourceCodeInfo$Properties} message SourceCodeInfo message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.SourceCodeInfo$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified SourceCodeInfo message, length delimited. Does not implicitly {@link google.protobuf.SourceCodeInfo.verify|verify} messages.
             * @param {google.protobuf.SourceCodeInfo$Properties} message SourceCodeInfo message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.SourceCodeInfo$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a SourceCodeInfo message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.SourceCodeInfo} SourceCodeInfo
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.SourceCodeInfo;

            /**
             * Decodes a SourceCodeInfo message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.SourceCodeInfo} SourceCodeInfo
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.SourceCodeInfo;

            /**
             * Verifies a SourceCodeInfo message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a SourceCodeInfo message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.SourceCodeInfo} SourceCodeInfo
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.SourceCodeInfo;

            /**
             * Creates a SourceCodeInfo message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.SourceCodeInfo.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.SourceCodeInfo} SourceCodeInfo
             */
            public static from(object: { [k: string]: any }): google.protobuf.SourceCodeInfo;

            /**
             * Creates a plain object from a SourceCodeInfo message. Also converts values to other types if specified.
             * @param {google.protobuf.SourceCodeInfo} message SourceCodeInfo
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.SourceCodeInfo, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this SourceCodeInfo message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this SourceCodeInfo to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        namespace SourceCodeInfo {

            type Location$Properties = {
                path?: number[];
                span?: number[];
                leading_comments?: string;
                trailing_comments?: string;
                leading_detached_comments?: string[];
            };

            /**
             * Constructs a new Location.
             * @exports google.protobuf.SourceCodeInfo.Location
             * @constructor
             * @param {google.protobuf.SourceCodeInfo.Location$Properties=} [properties] Properties to set
             */
            class Location {

                /**
                 * Constructs a new Location.
                 * @exports google.protobuf.SourceCodeInfo.Location
                 * @constructor
                 * @param {google.protobuf.SourceCodeInfo.Location$Properties=} [properties] Properties to set
                 */
                constructor(properties?: google.protobuf.SourceCodeInfo.Location$Properties);

                /**
                 * Location path.
                 * @type {Array.<number>}
                 */
                public path: number[];

                /**
                 * Location span.
                 * @type {Array.<number>}
                 */
                public span: number[];

                /**
                 * Location leading_comments.
                 * @type {string}
                 */
                public leading_comments: string;

                /**
                 * Location trailing_comments.
                 * @type {string}
                 */
                public trailing_comments: string;

                /**
                 * Location leading_detached_comments.
                 * @type {Array.<string>}
                 */
                public leading_detached_comments: string[];

                /**
                 * Creates a new Location instance using the specified properties.
                 * @param {google.protobuf.SourceCodeInfo.Location$Properties=} [properties] Properties to set
                 * @returns {google.protobuf.SourceCodeInfo.Location} Location instance
                 */
                public static create(properties?: google.protobuf.SourceCodeInfo.Location$Properties): google.protobuf.SourceCodeInfo.Location;

                /**
                 * Encodes the specified Location message. Does not implicitly {@link google.protobuf.SourceCodeInfo.Location.verify|verify} messages.
                 * @param {google.protobuf.SourceCodeInfo.Location$Properties} message Location message or plain object to encode
                 * @param {$protobuf.Writer} [writer] Writer to encode to
                 * @returns {$protobuf.Writer} Writer
                 */
                public static encode(message: google.protobuf.SourceCodeInfo.Location$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                /**
                 * Encodes the specified Location message, length delimited. Does not implicitly {@link google.protobuf.SourceCodeInfo.Location.verify|verify} messages.
                 * @param {google.protobuf.SourceCodeInfo.Location$Properties} message Location message or plain object to encode
                 * @param {$protobuf.Writer} [writer] Writer to encode to
                 * @returns {$protobuf.Writer} Writer
                 */
                public static encodeDelimited(message: google.protobuf.SourceCodeInfo.Location$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                /**
                 * Decodes a Location message from the specified reader or buffer.
                 * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                 * @param {number} [length] Message length if known beforehand
                 * @returns {google.protobuf.SourceCodeInfo.Location} Location
                 * @throws {Error} If the payload is not a reader or valid buffer
                 * @throws {$protobuf.util.ProtocolError} If required fields are missing
                 */
                public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.SourceCodeInfo.Location;

                /**
                 * Decodes a Location message from the specified reader or buffer, length delimited.
                 * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                 * @returns {google.protobuf.SourceCodeInfo.Location} Location
                 * @throws {Error} If the payload is not a reader or valid buffer
                 * @throws {$protobuf.util.ProtocolError} If required fields are missing
                 */
                public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.SourceCodeInfo.Location;

                /**
                 * Verifies a Location message.
                 * @param {Object.<string,*>} message Plain object to verify
                 * @returns {?string} `null` if valid, otherwise the reason why it is not
                 */
                public static verify(message: { [k: string]: any }): string;

                /**
                 * Creates a Location message from a plain object. Also converts values to their respective internal types.
                 * @param {Object.<string,*>} object Plain object
                 * @returns {google.protobuf.SourceCodeInfo.Location} Location
                 */
                public static fromObject(object: { [k: string]: any }): google.protobuf.SourceCodeInfo.Location;

                /**
                 * Creates a Location message from a plain object. Also converts values to their respective internal types.
                 * This is an alias of {@link google.protobuf.SourceCodeInfo.Location.fromObject}.
                 * @function
                 * @param {Object.<string,*>} object Plain object
                 * @returns {google.protobuf.SourceCodeInfo.Location} Location
                 */
                public static from(object: { [k: string]: any }): google.protobuf.SourceCodeInfo.Location;

                /**
                 * Creates a plain object from a Location message. Also converts values to other types if specified.
                 * @param {google.protobuf.SourceCodeInfo.Location} message Location
                 * @param {$protobuf.ConversionOptions} [options] Conversion options
                 * @returns {Object.<string,*>} Plain object
                 */
                public static toObject(message: google.protobuf.SourceCodeInfo.Location, options?: $protobuf.ConversionOptions): { [k: string]: any };

                /**
                 * Creates a plain object from this Location message. Also converts values to other types if specified.
                 * @param {$protobuf.ConversionOptions} [options] Conversion options
                 * @returns {Object.<string,*>} Plain object
                 */
                public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

                /**
                 * Converts this Location to JSON.
                 * @returns {Object.<string,*>} JSON object
                 */
                public toJSON(): { [k: string]: any };
            }
        }

        type GeneratedCodeInfo$Properties = {
            annotation?: google.protobuf.GeneratedCodeInfo.Annotation$Properties[];
        };

        /**
         * Constructs a new GeneratedCodeInfo.
         * @exports google.protobuf.GeneratedCodeInfo
         * @constructor
         * @param {google.protobuf.GeneratedCodeInfo$Properties=} [properties] Properties to set
         */
        class GeneratedCodeInfo {

            /**
             * Constructs a new GeneratedCodeInfo.
             * @exports google.protobuf.GeneratedCodeInfo
             * @constructor
             * @param {google.protobuf.GeneratedCodeInfo$Properties=} [properties] Properties to set
             */
            constructor(properties?: google.protobuf.GeneratedCodeInfo$Properties);

            /**
             * GeneratedCodeInfo annotation.
             * @type {Array.<google.protobuf.GeneratedCodeInfo.Annotation$Properties>}
             */
            public annotation: google.protobuf.GeneratedCodeInfo.Annotation$Properties[];

            /**
             * Creates a new GeneratedCodeInfo instance using the specified properties.
             * @param {google.protobuf.GeneratedCodeInfo$Properties=} [properties] Properties to set
             * @returns {google.protobuf.GeneratedCodeInfo} GeneratedCodeInfo instance
             */
            public static create(properties?: google.protobuf.GeneratedCodeInfo$Properties): google.protobuf.GeneratedCodeInfo;

            /**
             * Encodes the specified GeneratedCodeInfo message. Does not implicitly {@link google.protobuf.GeneratedCodeInfo.verify|verify} messages.
             * @param {google.protobuf.GeneratedCodeInfo$Properties} message GeneratedCodeInfo message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encode(message: google.protobuf.GeneratedCodeInfo$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Encodes the specified GeneratedCodeInfo message, length delimited. Does not implicitly {@link google.protobuf.GeneratedCodeInfo.verify|verify} messages.
             * @param {google.protobuf.GeneratedCodeInfo$Properties} message GeneratedCodeInfo message or plain object to encode
             * @param {$protobuf.Writer} [writer] Writer to encode to
             * @returns {$protobuf.Writer} Writer
             */
            public static encodeDelimited(message: google.protobuf.GeneratedCodeInfo$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

            /**
             * Decodes a GeneratedCodeInfo message from the specified reader or buffer.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @param {number} [length] Message length if known beforehand
             * @returns {google.protobuf.GeneratedCodeInfo} GeneratedCodeInfo
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.GeneratedCodeInfo;

            /**
             * Decodes a GeneratedCodeInfo message from the specified reader or buffer, length delimited.
             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
             * @returns {google.protobuf.GeneratedCodeInfo} GeneratedCodeInfo
             * @throws {Error} If the payload is not a reader or valid buffer
             * @throws {$protobuf.util.ProtocolError} If required fields are missing
             */
            public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.GeneratedCodeInfo;

            /**
             * Verifies a GeneratedCodeInfo message.
             * @param {Object.<string,*>} message Plain object to verify
             * @returns {?string} `null` if valid, otherwise the reason why it is not
             */
            public static verify(message: { [k: string]: any }): string;

            /**
             * Creates a GeneratedCodeInfo message from a plain object. Also converts values to their respective internal types.
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.GeneratedCodeInfo} GeneratedCodeInfo
             */
            public static fromObject(object: { [k: string]: any }): google.protobuf.GeneratedCodeInfo;

            /**
             * Creates a GeneratedCodeInfo message from a plain object. Also converts values to their respective internal types.
             * This is an alias of {@link google.protobuf.GeneratedCodeInfo.fromObject}.
             * @function
             * @param {Object.<string,*>} object Plain object
             * @returns {google.protobuf.GeneratedCodeInfo} GeneratedCodeInfo
             */
            public static from(object: { [k: string]: any }): google.protobuf.GeneratedCodeInfo;

            /**
             * Creates a plain object from a GeneratedCodeInfo message. Also converts values to other types if specified.
             * @param {google.protobuf.GeneratedCodeInfo} message GeneratedCodeInfo
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public static toObject(message: google.protobuf.GeneratedCodeInfo, options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Creates a plain object from this GeneratedCodeInfo message. Also converts values to other types if specified.
             * @param {$protobuf.ConversionOptions} [options] Conversion options
             * @returns {Object.<string,*>} Plain object
             */
            public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

            /**
             * Converts this GeneratedCodeInfo to JSON.
             * @returns {Object.<string,*>} JSON object
             */
            public toJSON(): { [k: string]: any };
        }

        namespace GeneratedCodeInfo {

            type Annotation$Properties = {
                path?: number[];
                source_file?: string;
                begin?: number;
                end?: number;
            };

            /**
             * Constructs a new Annotation.
             * @exports google.protobuf.GeneratedCodeInfo.Annotation
             * @constructor
             * @param {google.protobuf.GeneratedCodeInfo.Annotation$Properties=} [properties] Properties to set
             */
            class Annotation {

                /**
                 * Constructs a new Annotation.
                 * @exports google.protobuf.GeneratedCodeInfo.Annotation
                 * @constructor
                 * @param {google.protobuf.GeneratedCodeInfo.Annotation$Properties=} [properties] Properties to set
                 */
                constructor(properties?: google.protobuf.GeneratedCodeInfo.Annotation$Properties);

                /**
                 * Annotation path.
                 * @type {Array.<number>}
                 */
                public path: number[];

                /**
                 * Annotation source_file.
                 * @type {string}
                 */
                public source_file: string;

                /**
                 * Annotation begin.
                 * @type {number}
                 */
                public begin: number;

                /**
                 * Annotation end.
                 * @type {number}
                 */
                public end: number;

                /**
                 * Creates a new Annotation instance using the specified properties.
                 * @param {google.protobuf.GeneratedCodeInfo.Annotation$Properties=} [properties] Properties to set
                 * @returns {google.protobuf.GeneratedCodeInfo.Annotation} Annotation instance
                 */
                public static create(properties?: google.protobuf.GeneratedCodeInfo.Annotation$Properties): google.protobuf.GeneratedCodeInfo.Annotation;

                /**
                 * Encodes the specified Annotation message. Does not implicitly {@link google.protobuf.GeneratedCodeInfo.Annotation.verify|verify} messages.
                 * @param {google.protobuf.GeneratedCodeInfo.Annotation$Properties} message Annotation message or plain object to encode
                 * @param {$protobuf.Writer} [writer] Writer to encode to
                 * @returns {$protobuf.Writer} Writer
                 */
                public static encode(message: google.protobuf.GeneratedCodeInfo.Annotation$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                /**
                 * Encodes the specified Annotation message, length delimited. Does not implicitly {@link google.protobuf.GeneratedCodeInfo.Annotation.verify|verify} messages.
                 * @param {google.protobuf.GeneratedCodeInfo.Annotation$Properties} message Annotation message or plain object to encode
                 * @param {$protobuf.Writer} [writer] Writer to encode to
                 * @returns {$protobuf.Writer} Writer
                 */
                public static encodeDelimited(message: google.protobuf.GeneratedCodeInfo.Annotation$Properties, writer?: $protobuf.Writer): $protobuf.Writer;

                /**
                 * Decodes an Annotation message from the specified reader or buffer.
                 * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                 * @param {number} [length] Message length if known beforehand
                 * @returns {google.protobuf.GeneratedCodeInfo.Annotation} Annotation
                 * @throws {Error} If the payload is not a reader or valid buffer
                 * @throws {$protobuf.util.ProtocolError} If required fields are missing
                 */
                public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): google.protobuf.GeneratedCodeInfo.Annotation;

                /**
                 * Decodes an Annotation message from the specified reader or buffer, length delimited.
                 * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                 * @returns {google.protobuf.GeneratedCodeInfo.Annotation} Annotation
                 * @throws {Error} If the payload is not a reader or valid buffer
                 * @throws {$protobuf.util.ProtocolError} If required fields are missing
                 */
                public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): google.protobuf.GeneratedCodeInfo.Annotation;

                /**
                 * Verifies an Annotation message.
                 * @param {Object.<string,*>} message Plain object to verify
                 * @returns {?string} `null` if valid, otherwise the reason why it is not
                 */
                public static verify(message: { [k: string]: any }): string;

                /**
                 * Creates an Annotation message from a plain object. Also converts values to their respective internal types.
                 * @param {Object.<string,*>} object Plain object
                 * @returns {google.protobuf.GeneratedCodeInfo.Annotation} Annotation
                 */
                public static fromObject(object: { [k: string]: any }): google.protobuf.GeneratedCodeInfo.Annotation;

                /**
                 * Creates an Annotation message from a plain object. Also converts values to their respective internal types.
                 * This is an alias of {@link google.protobuf.GeneratedCodeInfo.Annotation.fromObject}.
                 * @function
                 * @param {Object.<string,*>} object Plain object
                 * @returns {google.protobuf.GeneratedCodeInfo.Annotation} Annotation
                 */
                public static from(object: { [k: string]: any }): google.protobuf.GeneratedCodeInfo.Annotation;

                /**
                 * Creates a plain object from an Annotation message. Also converts values to other types if specified.
                 * @param {google.protobuf.GeneratedCodeInfo.Annotation} message Annotation
                 * @param {$protobuf.ConversionOptions} [options] Conversion options
                 * @returns {Object.<string,*>} Plain object
                 */
                public static toObject(message: google.protobuf.GeneratedCodeInfo.Annotation, options?: $protobuf.ConversionOptions): { [k: string]: any };

                /**
                 * Creates a plain object from this Annotation message. Also converts values to other types if specified.
                 * @param {$protobuf.ConversionOptions} [options] Conversion options
                 * @returns {Object.<string,*>} Plain object
                 */
                public toObject(options?: $protobuf.ConversionOptions): { [k: string]: any };

                /**
                 * Converts this Annotation to JSON.
                 * @returns {Object.<string,*>} JSON object
                 */
                public toJSON(): { [k: string]: any };
            }
        }
    }
}
