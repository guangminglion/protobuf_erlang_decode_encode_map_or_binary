
%% Copyright (c) 2009
%% Nick Gerakines <nick@gerakines.net>
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
%%
%% @doc A protcol buffers encoding and decoding module.
-module(protobuffs).

%% Public
-export([encode/3, encode_packed/3,encode_packed_bin/3, decode/2,decode_bin/2, decode_packed/2,decode_packed_bin/2,encode_bin/3]).

%% Used by generated *_pb file. Not intended to used by User
-export([next_field_num/1, skip_next_field/1,skip_next_field_bin/1]).
-export([encode_varint/1, decode_varint/1]).

%% Will be removed from export, only intended for internal usage
-deprecated([{read_field_num_and_wire_type,1,next_version}]).
-deprecated([{decode_value,3,next_version}]).
-export([read_field_num_and_wire_type/1, decode_value/3,encode_internal_bin/3]).

-define(TYPE_VARINT, 0).
-define(TYPE_64BIT, 1).
-define(TYPE_STRING, 2).
-define(TYPE_START_GROUP, 3).
-define(TYPE_END_GROUP, 4).
-define(TYPE_32BIT, 5).

-type encoded_field_type() ::
	?TYPE_VARINT | ?TYPE_64BIT | ?TYPE_STRING |
	?TYPE_START_GROUP | ?TYPE_END_GROUP | ?TYPE_32BIT.

-type field_type() :: bool | enum | int32 | uint32 | int64 |
		      unit64 | sint32 | sint64 | fixed32 |
		      sfixed32 | fixed64 | sfixed64 | string |
		      bytes | float | double.

%%--------------------------------------------------------------------
%% @doc Encode an Erlang data structure into a Protocol Buffers value.
%% @end
%%--------------------------------------------------------------------
-spec encode(FieldID :: non_neg_integer(),
	     Value :: any(),
	     Type :: field_type()) ->
		    iodata().
encode(FieldID, Value, Type) ->
    encode_internal(FieldID, Value, Type).

-spec encode_bin(FieldID :: non_neg_integer(),
         Value :: any(),
         Type :: field_type()) ->
            iodata().
encode_bin(FieldID, Value, Type) ->
    %%  
   R  = encode_internal_bin(FieldID, Value, Type),
   %%  error_logger:info_msg("~p ~n",[{FieldID, Value, Type,R}]),
   R.
binary_to_float2(Float) ->
    %%error_logger:info_msg("binary_to_float2 ~p ~n",[{Float}]),
    try binary_to_float(Float) of
        B ->
            <<B:32/little-float>>
    catch
        _:_ ->
                try binary_to_integer(Float) of
                    R ->
                        <<R:32/little-float>>
                catch
                    _:_ ->
                        Float
                end
    end.
binary_to_float3(Float) ->
    %%error_logger:info_msg("binary_to_float3 ~p ~n",[{Float}]),
    try binary_to_float(Float) of
        B ->
            <<B:64/little-float>>
    catch
        _:_ ->
                try binary_to_integer(Float) of
                    R ->
                        <<R:64/little-float>>
                catch
                    _:_ ->
                        Float
                end
    end.
%%--------------------------------------------------------------------
%% @doc Encode an list of Erlang data structure into a Protocol Buffers values.
%% @end
%%--------------------------------------------------------------------
-spec encode_packed(FieldID :: non_neg_integer(),
		    Values :: list(),
		    Type :: field_type()) ->
			   binary().
encode_packed(_FieldID, [], _Type) ->
    <<>>;
encode_packed(FieldID, Values, Type) ->
    PackedValues = encode_packed_internal(Values,Type,[]),
    Size = encode_varint(iolist_size(PackedValues)),
    [encode_field_tag(FieldID, ?TYPE_STRING),Size,PackedValues].

%%bin
-spec encode_packed_bin(FieldID :: non_neg_integer(),
            Values :: list(),
            Type :: field_type()) ->
               binary().
encode_packed_bin(_FieldID, [], _Type) ->
    <<>>;
encode_packed_bin(FieldID, Values, Type) ->
    PackedValues = encode_packed_internal_bin(Values,Type,[]),
    Size = encode_varint(iolist_size(PackedValues)),
    [encode_field_tag(FieldID, ?TYPE_STRING),Size,PackedValues].


%% @hidden
-spec encode_internal(FieldID :: non_neg_integer(),
		      Value :: any(),
		      Type :: field_type()) ->
			     iolist().
encode_internal(FieldID, false, bool) ->
    encode_internal(FieldID, 0, int32);
encode_internal(FieldID, true, bool) ->
    encode_internal(FieldID, 1, int32);
encode_internal(FieldID, Integer, enum) when is_integer(Integer) ->
    encode_internal(FieldID, Integer, int32);
encode_internal(FieldID, Integer, int32) when Integer >= -16#80000000,
					      Integer < 0 ->
    encode_internal(FieldID, Integer, int64);
encode_internal(FieldID, Integer, int64) when Integer >= -16#8000000000000000,
					      Integer < 0 ->
    encode_internal(FieldID, Integer + (1 bsl 64), uint64);
encode_internal(FieldID, Integer, int32) when is_integer(Integer),
					      Integer >= -16#80000000,
					      Integer =< 16#7fffffff ->
    encode_varint_field(FieldID, Integer);
encode_internal(FieldID, Integer, uint32) when Integer band 16#ffffffff =:= Integer ->
    encode_varint_field(FieldID, Integer);
encode_internal(FieldID, Integer, int64) when is_integer(Integer),
					      Integer >= -16#8000000000000000,
					      Integer =< 16#7fffffffffffffff ->
    encode_varint_field(FieldID, Integer);
encode_internal(FieldID, Integer, uint64) when Integer band 16#ffffffffffffffff =:= Integer ->
    encode_varint_field(FieldID, Integer);
encode_internal(FieldID, Integer, bool) when Integer =:= 1; Integer =:= 0 ->
    encode_varint_field(FieldID, Integer);
encode_internal(FieldID, Integer, sint32) when is_integer(Integer),
					       Integer >= -16#80000000,
					       Integer < 0 ->
    encode_varint_field(FieldID, bnot (Integer bsl 1));
encode_internal(FieldID, Integer, sint64) when is_integer(Integer),
					       Integer >= -16#8000000000000000,
					       Integer < 0 ->
    encode_varint_field(FieldID, bnot (Integer bsl 1));
encode_internal(FieldID, Integer, sint32) when is_integer(Integer),
					       Integer >= 0,
					       Integer =< 16#7fffffff ->
    encode_varint_field(FieldID, Integer bsl 1);
encode_internal(FieldID, Integer, sint64) when is_integer(Integer),
					       Integer >= 0,
					       Integer =< 16#7fffffffffffffff ->
    encode_varint_field(FieldID, Integer bsl 1);
    
encode_internal(FieldID, Integer, fixed32) when Integer band 16#ffffffff =:= Integer ->
    [encode_field_tag(FieldID, ?TYPE_32BIT), <<Integer:32/little-integer>>];
encode_internal(FieldID, Integer, sfixed32) when Integer >= -16#80000000,
						 Integer =< 16#7fffffff ->
    [encode_field_tag(FieldID, ?TYPE_32BIT), <<Integer:32/little-integer>>];
encode_internal(FieldID, Integer, fixed64) when Integer band 16#ffffffffffffffff =:= Integer ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), <<Integer:64/little-integer>>];
encode_internal(FieldID, Integer, sfixed64) when Integer >= -16#8000000000000000,
						 Integer =< 16#7fffffffffffffff ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), <<Integer:64/little-integer>>];
encode_internal(FieldID, String, string) when is_list(String) ->
    encode_internal(FieldID, unicode:characters_to_binary(String), string);
encode_internal(FieldID, String, string) when is_binary(String) ->
    encode_internal(FieldID, String, bytes);
encode_internal(FieldID, Bytes, bytes) when is_binary(Bytes); is_list(Bytes) ->
    [encode_field_tag(FieldID, ?TYPE_STRING), encode_varint(iolist_size(Bytes)), Bytes];
encode_internal(FieldID, Float, float) when is_integer(Float) ->
    encode_internal(FieldID, Float + 0.0, float);
encode_internal(FieldID, Float, float) when is_float(Float) ->
    [encode_field_tag(FieldID, ?TYPE_32BIT), <<Float:32/little-float>>];
encode_internal(FieldID, nan, float) ->
    [encode_field_tag(FieldID, ?TYPE_32BIT), <<0:16,192:8,255:8>>];
encode_internal(FieldID, infinity, float) ->
    [encode_field_tag(FieldID, ?TYPE_32BIT), <<0:16,128:8,127:8>>];
encode_internal(FieldID, '-infinity', float) ->
    [encode_field_tag(FieldID, ?TYPE_32BIT), <<0:16,128:8,255:8>>];
encode_internal(FieldID, Float, double) when is_integer(Float) ->
    encode_internal(FieldID, Float + 0.0, double);
encode_internal(FieldID, Float, double) when is_float(Float) ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), <<Float:64/little-float>>];
encode_internal(FieldID, nan, double) ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), <<0:48,16#F8,16#FF>>];
encode_internal(FieldID, infinity, double) ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), <<0:48,16#F0,16#7F>>];
encode_internal(FieldID, '-infinity', double) ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), <<0:48,16#F0,16#FF>>];
encode_internal(FieldID, Value, Type) ->
    erlang:error(badarg,[FieldID, Value, Type]).

%%@ebin
-spec encode_internal_bin(FieldID :: non_neg_integer(),
              Value :: any(),
              Type :: field_type()) ->
                 iolist().
encode_internal_bin(FieldID, false, bool) ->
    encode_internal_bin(FieldID, 0, int32);
encode_internal_bin(FieldID, true, bool) ->
    encode_internal_bin(FieldID, 1, int32);
encode_internal_bin(FieldID, Integer, enum)->
    encode_internal_bin(FieldID, Integer, int32);
encode_internal_bin(FieldID, Integer, int32) ->
    encode_varint_field_bin(FieldID, Integer,int32);
encode_internal_bin(FieldID, Integer, uint32) ->
    encode_varint_field_bin(FieldID, Integer,uint32);
encode_internal_bin(FieldID, Integer, int64)->
    encode_varint_field_bin(FieldID, Integer,int64);
encode_internal_bin(FieldID, Integer, uint64)->
    encode_varint_field_bin(FieldID, Integer,uint64);
encode_internal_bin(FieldID, Integer, bool)  ->
    encode_varint_field_bin(FieldID, Integer,bool);
encode_internal_bin(FieldID, Integer, sint32) ->
    encode_varint_field_bin(FieldID,Integer,sint32);
encode_internal_bin(FieldID, Integer, sint64)->
    encode_varint_field_bin(FieldID, Integer,sint64);
encode_internal_bin(FieldID, Integer, fixed32)->
    [encode_field_tag(FieldID, ?TYPE_32BIT),Integer];
encode_internal_bin(FieldID, Integer, sfixed32) ->
    [encode_field_tag(FieldID, ?TYPE_32BIT), Integer];
encode_internal_bin(FieldID, Integer, fixed64) ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), Integer];
encode_internal_bin(FieldID, Integer, sfixed64) ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), Integer];
encode_internal_bin(FieldID, String, string) when is_list(String) ->
    encode_internal(FieldID, unicode:characters_to_binary(String), string);
encode_internal_bin(FieldID, String, string) when is_binary(String) ->
    encode_internal_bin(FieldID, String, bytes);
encode_internal_bin(FieldID, Bytes, bytes) when is_binary(Bytes); is_list(Bytes) ->
    [encode_field_tag(FieldID, ?TYPE_STRING), encode_varint(iolist_size(Bytes)), Bytes];
encode_internal_bin(FieldID, Float, float) when is_integer(Float) ->
    [encode_field_tag(FieldID, ?TYPE_32BIT),<<Float:32/little-float>>]; 
encode_internal_bin(FieldID, Float, float) when is_float(Float) ->        
     [encode_field_tag(FieldID, ?TYPE_32BIT), <<Float:32/little-float>>];    
encode_internal_bin(FieldID, Float, float) ->
   [encode_field_tag(FieldID, ?TYPE_32BIT),Float];
encode_internal_bin(FieldID, nan, float) ->
    [encode_field_tag(FieldID, ?TYPE_32BIT), <<0:16,192:8,255:8>>];
encode_internal_bin(FieldID, infinity, float) ->
    [encode_field_tag(FieldID, ?TYPE_32BIT), <<0:16,128:8,127:8>>];
encode_internal_bin(FieldID, '-infinity', float) ->
    [encode_field_tag(FieldID, ?TYPE_32BIT), <<0:16,128:8,255:8>>];
encode_internal_bin(FieldID, Float, double) when is_float(Float) ->
    [encode_field_tag(FieldID, ?TYPE_64BIT),<<Float:64/little-float>>];
encode_internal_bin(FieldID,  Float, double) ->    
    [encode_field_tag(FieldID, ?TYPE_64BIT),Float];
encode_internal_bin(FieldID, nan, double) ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), <<0:48,16#F8,16#FF>>];
encode_internal_bin(FieldID, infinity, double) ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), <<0:48,16#F0,16#7F>>];
encode_internal_bin(FieldID, '-infinity', double) ->
    [encode_field_tag(FieldID, ?TYPE_64BIT), <<0:48,16#F0,16#FF>>];
encode_internal_bin(FieldID, Value, Type) ->
    erlang:error(badarg,[FieldID, Value, Type]).

%% @hidden
-spec encode_packed_internal(Values :: list(),
			     ExpectedType :: field_type(),
			     Acc :: list()) ->
				    iolist().
encode_packed_internal([],_Type,Acc) ->
    lists:reverse(Acc);
encode_packed_internal([Value|Tail], ExpectedType, Acc) ->
    [_|V] = encode_internal(1, Value, ExpectedType),
    encode_packed_internal(Tail, ExpectedType, [V|Acc]).


-spec encode_packed_internal_bin(Values :: list(),
                 ExpectedType :: field_type(),
                 Acc :: list()) ->
                    iolist().
encode_packed_internal_bin([],_Type,Acc) ->
    lists:reverse(Acc);
encode_packed_internal_bin([Value|Tail], ExpectedType, Acc) ->
    [_|V] = encode_internal_bin(1, Value, ExpectedType),
    encode_packed_internal_bin(Tail, ExpectedType, [V|Acc]).

%%--------------------------------------------------------------------
%% @doc Will be hidden in future releases
%% @end
%%--------------------------------------------------------------------
-spec read_field_num_and_wire_type(Bytes :: binary()) ->
					  {{non_neg_integer(), encoded_field_type()}, binary()}.
read_field_num_and_wire_type(<<_:8,_/binary>> = Bytes) ->
    {Tag, Rest} = decode_varint(Bytes),
    FieldID = Tag bsr 3,
    case Tag band 7 of
	?TYPE_VARINT ->
	    {{FieldID, ?TYPE_VARINT}, Rest};
	?TYPE_64BIT ->
	    {{FieldID, ?TYPE_64BIT}, Rest};
	?TYPE_STRING ->
	    {{FieldID, ?TYPE_STRING}, Rest};
	?TYPE_START_GROUP ->
	    {{FieldID, ?TYPE_START_GROUP}, Rest};
	?TYPE_END_GROUP ->
	    {{FieldID, ?TYPE_END_GROUP}, Rest};
	?TYPE_32BIT ->
	    {{FieldID, ?TYPE_32BIT}, Rest};
	_Else ->
	    erlang:throw({error, "Error decodeing type",Tag, Rest})
    end;
read_field_num_and_wire_type(Bytes) ->
    erlang:error(badarg,[Bytes]).

%%--------------------------------------------------------------------
%% @doc Decode a single value from a protobuffs data structure
%% @end
%%--------------------------------------------------------------------
-spec decode(Bytes :: binary(), ExpectedType :: field_type()) ->
		    {{non_neg_integer(), any()}, binary()}.
decode(Bytes, ExpectedType) ->
    {{FieldID, WireType}, Rest} = read_field_num_and_wire_type(Bytes),
    {Value, Rest1} = decode_value(Rest, WireType, ExpectedType),
    {{FieldID, Value}, Rest1}.
-spec decode_bin(Bytes :: binary(), ExpectedType :: field_type()) ->
            {{non_neg_integer(), any()}, binary()}.
decode_bin(Bytes, ExpectedType) ->
    {{FieldID, WireType}, Rest} = read_field_num_and_wire_type(Bytes),
    {Value, Rest1} = decode_value_bin(Rest, WireType, ExpectedType),
  %%  error_logger:info_report([{"decode_bin----------------",FieldID,Rest, WireType, ExpectedType,Value}]),
    {{FieldID, Value}, Rest1}.
%%--------------------------------------------------------------------
%% @doc Decode packed values from a protobuffs data structure
%% @end
%%--------------------------------------------------------------------
-spec decode_packed(Bytes :: binary(), ExpectedType :: field_type()) ->
			   {{non_neg_integer(), any()}, binary()}.
decode_packed(Bytes, ExpectedType) ->
    case read_field_num_and_wire_type(Bytes) of
	{{FieldID, ?TYPE_STRING}, Rest} ->
	    {Length, Rest1} = decode_varint(Rest),
	    {Packed,Rest2} = split_binary(Rest1, Length),
	    Values = decode_packed_values(Packed, ExpectedType, []),
	    {{FieldID, Values},Rest2};
	_Else ->
	    erlang:error(badarg)
    end.
%%bin
-spec decode_packed_bin(Bytes :: binary(), ExpectedType :: field_type()) ->
               {{non_neg_integer(), any()}, binary()}.
decode_packed_bin(Bytes, ExpectedType) ->
    case read_field_num_and_wire_type(Bytes) of
    {{FieldID, ?TYPE_STRING}, Rest} ->
        {Length, Rest1} = decode_varint(Rest),
        {Packed,Rest2} = split_binary(Rest1, Length),
        Values = decode_packed_values_bin(Packed, ExpectedType, []),
        {{FieldID, Values},Rest2};
    _Else ->
        erlang:error(badarg)
    end.
%%--------------------------------------------------------------------
%% @doc Returns the next field number id from a protobuffs data structure
%% @end
%%--------------------------------------------------------------------
-spec next_field_num(Bytes :: binary()) -> {ok,non_neg_integer()}.
next_field_num(Bytes) ->
    {{FieldID,_WiredType}, _Rest} = read_field_num_and_wire_type(Bytes),
    {ok,FieldID}.

%%--------------------------------------------------------------------
%% @doc Skips the field at the front of the message, effectively ignoring it.
%% @end
%%--------------------------------------------------------------------
-spec skip_next_field_bin(Bytes :: binary()) ->
                             {ok, binary()}.
skip_next_field_bin(Bytes) ->
    {{_FieldId, WireType}, Rest} = read_field_num_and_wire_type(Bytes),
    case WireType of
        ?TYPE_VARINT ->
            {_, Rest1} = decode_varint(Rest);
        ?TYPE_64BIT ->
            <<_:64, Rest1/binary>> = Rest;
        ?TYPE_32BIT ->
            <<_:32, Rest1/binary>> = Rest;
        ?TYPE_STRING ->
            {_, Rest1} = decode_value_bin(Rest, WireType, string);
        _ ->
            Rest1 = Rest
    end,
    {ok, Rest1}.
-spec skip_next_field(Bytes :: binary()) ->
                             {ok, binary()}.
skip_next_field(Bytes) ->
    {{_FieldId, WireType}, Rest} = read_field_num_and_wire_type(Bytes),
    case WireType of
        ?TYPE_VARINT ->
            {_, Rest1} = decode_varint(Rest);
        ?TYPE_64BIT ->
            <<_:64, Rest1/binary>> = Rest;
        ?TYPE_32BIT ->
            <<_:32, Rest1/binary>> = Rest;
        ?TYPE_STRING ->
            {_, Rest1} = decode_value(Rest, WireType, string);
        _ ->
            Rest1 = Rest
    end,
    {ok, Rest1}.
%% @hidden
-spec decode_packed_values(Bytes :: binary(),
			   Type :: field_type(),
			   Acc :: list()) ->
				  iolist().
decode_packed_values(<<>>, _, Acc) ->
    lists:reverse(Acc);
decode_packed_values(Bytes, bool, Acc) ->
    {Value,Rest} = decode_value(Bytes,?TYPE_VARINT, bool),
    decode_packed_values(Rest, bool, [Value|Acc]);
decode_packed_values(Bytes, enum, Acc) ->
    {Value,Rest} = decode_value(Bytes,?TYPE_VARINT, enum),
    decode_packed_values(Rest, enum, [Value|Acc]);
decode_packed_values(Bytes, int32, Acc) ->
    {Value,Rest} = decode_value(Bytes,?TYPE_VARINT, int32),
    decode_packed_values(Rest, int32, [Value|Acc]);
decode_packed_values(Bytes, uint32, Acc) ->
    {Value,Rest} = decode_value(Bytes,?TYPE_VARINT, uint32),
    decode_packed_values(Rest, uint32, [Value|Acc]);
decode_packed_values(Bytes, sint32, Acc) ->
    {Value,Rest} = decode_value(Bytes,?TYPE_VARINT, sint32),
    decode_packed_values(Rest, sint32, [Value|Acc]);
decode_packed_values(Bytes, int64, Acc) ->
    {Value,Rest} = decode_value(Bytes,?TYPE_VARINT, int64),
    decode_packed_values(Rest, int64, [Value|Acc]);
decode_packed_values(Bytes, uint64, Acc) ->
    {Value,Rest} = decode_value(Bytes,?TYPE_VARINT, uint64),
    decode_packed_values(Rest, uint64, [Value|Acc]);
decode_packed_values(Bytes, sint64, Acc) ->
    {Value,Rest} = decode_value(Bytes,?TYPE_VARINT, sint64),
    decode_packed_values(Rest, sint64, [Value|Acc]);
decode_packed_values(Bytes, float, Acc) ->
    {Value,Rest} = decode_value(Bytes,?TYPE_32BIT, float),
    decode_packed_values(Rest, float, [Value|Acc]);
decode_packed_values(Bytes, double, Acc) ->
    {Value,Rest} = decode_value(Bytes,?TYPE_64BIT, double),
    decode_packed_values(Rest, double, [Value|Acc]);
decode_packed_values(Bytes, Type, Acc) ->
    erlang:error(badarg,[Bytes,Type,Acc]).
%% @hidden
-spec decode_packed_values_bin(Bytes :: binary(),
               Type :: field_type(),
               Acc :: list()) ->
                  iolist().
decode_packed_values_bin(<<>>, _, Acc) ->
    lists:reverse(Acc);
decode_packed_values_bin(Bytes, bool, Acc) ->
    {Value,Rest} = decode_value_bin(Bytes,?TYPE_VARINT, bool),
    decode_packed_values_bin(Rest, bool, [Value|Acc]);
decode_packed_values_bin(Bytes, enum, Acc) ->
    {Value,Rest} = decode_value_bin(Bytes,?TYPE_VARINT, enum),
    decode_packed_values_bin(Rest, enum, [Value|Acc]);
decode_packed_values_bin(Bytes, int32, Acc) ->
    {Value,Rest} = decode_value_bin(Bytes,?TYPE_VARINT, int32),
    decode_packed_values_bin(Rest, int32, [Value|Acc]);
decode_packed_values_bin(Bytes, uint32, Acc) ->
    {Value,Rest} = decode_value_bin(Bytes,?TYPE_VARINT, uint32),
    decode_packed_values_bin(Rest, uint32, [Value|Acc]);
decode_packed_values_bin(Bytes, sint32, Acc) ->
    {Value,Rest} = decode_value_bin(Bytes,?TYPE_VARINT, sint32),
    decode_packed_values_bin(Rest, sint32, [Value|Acc]);
decode_packed_values_bin(Bytes, int64, Acc) ->
    {Value,Rest} = decode_value_bin(Bytes,?TYPE_VARINT, int64),
    decode_packed_values_bin(Rest, int64, [Value|Acc]);
decode_packed_values_bin(Bytes, uint64, Acc) ->
    {Value,Rest} = decode_value_bin(Bytes,?TYPE_VARINT, uint64),
    decode_packed_values_bin(Rest, uint64, [Value|Acc]);
decode_packed_values_bin(Bytes, sint64, Acc) ->
    {Value,Rest} = decode_value_bin(Bytes,?TYPE_VARINT, sint64),
    decode_packed_values_bin(Rest, sint64, [Value|Acc]);
decode_packed_values_bin(Bytes, float, Acc) ->
    {Value,Rest} = decode_value_bin(Bytes,?TYPE_32BIT, float),
    decode_packed_values_bin(Rest, float, [Value|Acc]);
decode_packed_values_bin(Bytes, double, Acc) ->
    {Value,Rest} = decode_value_bin(Bytes,?TYPE_64BIT, double),
    decode_packed_values_bin(Rest, double, [Value|Acc]);
decode_packed_values_bin(Bytes, Type, Acc) ->
    erlang:error(badarg,[Bytes,Type,Acc]).

%%--------------------------------------------------------------------
%% @doc Will be hidden in future releases
%% @end
%%--------------------------------------------------------------------
-spec decode_value(Bytes :: binary(),
		   WireType :: encoded_field_type(),
		   ExpectedType :: field_type()) ->
			  {any(),binary()}.
decode_value(Bytes, ?TYPE_VARINT, ExpectedType) ->
    {Value, Rest} = decode_varint(Bytes),
    {typecast(Value, ExpectedType), Rest};
decode_value(Bytes, ?TYPE_STRING, string) ->
    {Length, Rest} = decode_varint(Bytes),
    {Value,Rest1} = split_binary(Rest, Length),
    {[C || <<C/utf8>> <= Value],Rest1};
decode_value(Bytes, ?TYPE_STRING, bytes) ->
    {Length, Rest} = decode_varint(Bytes),
    split_binary(Rest, Length);
decode_value(<<Value:64/little-unsigned-integer, Rest/binary>>, ?TYPE_64BIT, fixed64) ->
    {Value, Rest};
decode_value(<<Value:32/little-unsigned-integer, _:32, Rest/binary>>, ?TYPE_64BIT, fixed32) ->
    {Value, Rest};
decode_value(<<Value:64/little-signed-integer, Rest/binary>>, ?TYPE_64BIT, sfixed64) ->
    {Value, Rest};
decode_value(<<Value:32/little-signed-integer, _:32, Rest/binary>>, ?TYPE_64BIT, sfixed32) ->
    {Value, Rest};
decode_value(<<Value:32/little-unsigned-integer, Rest/binary>>, ?TYPE_32BIT, Type) when Type =:= fixed32; Type =:= fixed64 ->
    {Value, Rest};
decode_value(<<Value:32/little-signed-integer, Rest/binary>>, ?TYPE_32BIT, Type) when Type =:= sfixed32; Type =:= sfixed64 ->
    {Value, Rest};
decode_value(<<Value:32/little-float, Rest/binary>>, ?TYPE_32BIT, float) ->
    {Value + 0.0, Rest};
decode_value(<<0:16, 128:8, 127:8, Rest/binary>>, ?TYPE_32BIT, float) ->
    {infinity, Rest};
decode_value(<<0:16, 128:8, 255:8, Rest/binary>>, ?TYPE_32BIT, float) ->
    {'-infinity', Rest};
decode_value(<<_:16, 2#1:1, _:7, _:1, 2#1111111:7, Rest/binary>>, ?TYPE_32BIT, float) ->
    {nan, Rest};
decode_value(<<Value:64/little-float, Rest/binary>>, ?TYPE_64BIT, double) ->
    {Value + 0.0, Rest};
decode_value(<<0:48, 240:8, 127:8, Rest/binary>>, ?TYPE_64BIT, double) ->
    {infinity, Rest};
decode_value(<<0:48, 240:8, 255:8, Rest/binary>>, ?TYPE_64BIT, double) ->
    {'-infinity', Rest};
decode_value(<<_:48, 2#1111:4, _:4, _:1, 2#1111111:7, Rest/binary>>, ?TYPE_64BIT, double) ->
    {nan, Rest};
decode_value(Bytes,WireType,ExpectedType) ->
    erlang:error(badarg,[Bytes,WireType,ExpectedType]).







-spec decode_value_bin(Bytes :: binary(),
           WireType :: encoded_field_type(),
           ExpectedType :: field_type()) ->
              {any(),binary()}.
decode_value_bin(Bytes, ?TYPE_VARINT, ExpectedType) ->
    {Value, Rest} = decode_varint_bin(Bytes),
    {typecast_bin(Value, ExpectedType), Rest};
decode_value_bin(Bytes, ?TYPE_STRING, string) ->
    {Length, Rest} = decode_varint(Bytes),
    split_binary(Rest, Length);
decode_value_bin(Bytes, ?TYPE_STRING, bytes) ->
    {Length, Rest} = decode_varint(Bytes),
    split_binary(Rest, Length);
decode_value_bin(<<Value:64/little-unsigned-integer, Rest/binary>>, ?TYPE_64BIT, fixed64) ->
    {<<Value:64/little-unsigned-integer>>, Rest};
decode_value_bin(<<Value:32/little-unsigned-integer, _:32, Rest/binary>>, ?TYPE_64BIT, fixed32) ->
    {<<Value:32/little-unsigned-integer>>, Rest};
decode_value_bin(<<Value:64/little-signed-integer, Rest/binary>>, ?TYPE_64BIT, sfixed64) ->
    {<<Value:64/little-signed-integer>>, Rest};
decode_value_bin(<<Value:32/little-signed-integer, _:32, Rest/binary>>, ?TYPE_64BIT, sfixed32) ->
    {<<Value:32/little-signed-integer>>, Rest};
decode_value_bin(<<Value:32/little-unsigned-integer, Rest/binary>>, ?TYPE_32BIT, Type) when Type =:= fixed32; Type =:= fixed64 ->
    {<<Value:32/little-unsigned-integer>>, Rest};
decode_value_bin(<<Value:32/little-signed-integer, Rest/binary>>, ?TYPE_32BIT, Type) when Type =:= sfixed32; Type =:= sfixed64 ->
    {<<Value:32/little-signed-integer>>, Rest};
decode_value_bin(<<Value:32/little-float, Rest/binary>>, ?TYPE_32BIT, float) ->
    {<<Value:32/little-float>>, Rest};
decode_value_bin(<<0:16, 128:8, 127:8, Rest/binary>>, ?TYPE_32BIT, float) ->
    {<<0:16, 128:8, 127:8>>, Rest};
decode_value_bin(<<0:16, 128:8, 255:8, Rest/binary>>, ?TYPE_32BIT, float) ->
    {<<0:16, 128:8, 255:8>>, Rest};
decode_value_bin(<<N1:16, 2#1:1, N2:7, N3:1, 2#1111111:7, Rest/binary>>, ?TYPE_32BIT, float) ->
    {<<N1:16, 2#1:1, N2:7, N3:1, 2#1111111:7>>, Rest};
decode_value_bin(<<Value:64/little-float, Rest/binary>>, ?TYPE_64BIT, double) ->
    {<<Value:64/little-float>>, Rest};
decode_value_bin(<<0:48, 240:8, 127:8, Rest/binary>>, ?TYPE_64BIT, double) ->
    {<<0:48, 240:8, 127:8>>, Rest};
decode_value_bin(<<0:48, 240:8, 255:8, Rest/binary>>, ?TYPE_64BIT, double) ->
    {<<0:48, 240:8, 255:8>>, Rest};
decode_value_bin(<<N1:48, 2#1111:4, N2:4, N3:1, 2#1111111:7, Rest/binary>>, ?TYPE_64BIT, double) ->
    {<<N1:48, 2#1111:4, N2:4, N3:1, 2#1111111:7>>, Rest};
decode_value_bin(Bytes,WireType,ExpectedType) ->
    erlang:error(badarg,[Bytes,WireType,ExpectedType]).




%% @hidden
-spec typecast(Value :: any(), Type :: field_type()) ->
		      any().
typecast(Value, SignedType) when SignedType =:= int32; SignedType =:= int64; SignedType =:= enum ->
    if
        Value band 16#8000000000000000 =/= 0 -> Value - 16#10000000000000000;
        true -> Value
    end;
typecast(Value, SignedType) when SignedType =:= sint32; SignedType =:= sint64 ->
    (Value bsr 1) bxor (-(Value band 1));
typecast(Value, Type) when Type =:= bool ->
    Value =:= 1;
typecast(Value, _) ->
    Value.

-spec typecast_bin(Value :: any(), Type :: field_type()) ->
              any().
typecast_bin(Value, _) ->
    Value.

%% @hidden
-spec encode_field_tag(FieldID :: non_neg_integer(),
		       FieldType :: encoded_field_type()) ->
			      iodata().
encode_field_tag(FieldID, FieldType) when FieldID band 16#3fffffff =:= FieldID ->
    encode_varint((FieldID bsl 3) bor FieldType).

%% @hidden
-spec encode_varint_field(FieldID :: non_neg_integer(),
			  Integer :: integer()) ->
				 iolist().
encode_varint_field(FieldID, Integer) ->
    [encode_field_tag(FieldID, ?TYPE_VARINT), encode_varint(Integer)].



-spec encode_varint_field_bin(FieldID :: non_neg_integer(),
              Integer :: integer()|binary(),Type :: atom()) ->
                 iolist().
encode_varint_field_bin(FieldID, Integer,Type) when is_integer(Integer) ->
    encode_internal(FieldID,Integer,Type);
encode_varint_field_bin(FieldID, Integer,Type) ->
   [encode_field_tag(FieldID, ?TYPE_VARINT),Integer].

binary_to_integer2(Value) ->
    try binary_to_integer(Value) of
        R ->
              encode_varint(R)
    catch
        _:_  ->
              Value
    end.

%% @hidden
-spec encode_varint(I :: integer()) ->
			   iodata().
encode_varint(I) ->
    encode_varint(I, []).



%% @hidden
-spec encode_varint(I :: integer(), Acc :: list()) ->
			   iodata().
encode_varint(I, Acc) when I =< 16#7f ->
    lists:reverse([I | Acc]);
encode_varint(I, Acc) ->
    Last_Seven_Bits = (I - ((I bsr 7) bsl 7)),
    First_X_Bits = (I bsr 7),
    With_Leading_Bit = Last_Seven_Bits bor 16#80,
    encode_varint(First_X_Bits, [With_Leading_Bit|Acc]).



%% @hidden
-spec decode_varint(Bytes :: binary()) ->
			   {integer(), binary()}.
decode_varint(Bytes) ->
    decode_varint(Bytes, 0, 0).

-spec decode_varint_bin(Bytes :: binary()) ->
               {integer(), binary()}.
decode_varint_bin(Bytes) ->
    decode_varint_bin(Bytes, <<>>, 0).

%% @hidden
-spec decode_varint(Bytes :: binary(), non_neg_integer(), non_neg_integer()) ->
			   {integer(), binary()}.
decode_varint(<<0:1, I:7, Rest/binary>>, Int, Depth) ->
    {(I bsl Depth) bor Int, Rest};
decode_varint(<<1:1, I:7, Rest/binary>>, Int, Depth) ->
    decode_varint(Rest, (I bsl Depth) bor Int, Depth + 7);
decode_varint(Bin, Int, Depth) ->
    erlang:error(badarg, [Bin, Int, Depth]).


%% @hidden
-spec decode_varint_bin(Bytes :: binary(), binary(), non_neg_integer()) ->
               {integer(), binary()}.
decode_varint_bin(<<0:1, I:7, Rest/binary>>, Int, Depth) ->
    {<<Int/binary,0:1, I:7>>,Rest};
decode_varint_bin(<<1:1, I:7, Rest/binary>>, Int, Depth) ->
    DestINt = <<Int/binary,1:1, I:7>>,
    decode_varint_bin(Rest,DestINt, Depth + 7);
decode_varint_bin(Bin, Int, Depth) ->
    erlang:error(badarg, [Bin, Int, Depth]).